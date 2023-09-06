use std::sync::{
  atomic::{AtomicU64, Ordering},
  Arc,
};

use actix::Addr;
use anyhow::{Error, Result};
use bytes::Bytes;
use maxwell_protocol::PushReq;
use once_cell::sync::OnceCell;
use quick_cache::{sync::Cache, Weighter};
use seriesdb::{
  prelude::{Coder, Db, Table},
  table::TtlTable,
};

use crate::{
  db::MsgCoder,
  puller::{NotifyMsg, Puller},
};
use crate::{db::DB, puller::PullerMgr};

pub struct Pusher {
  topic: String,
  next_offset: AtomicU64,
  table: Arc<TtlTable>,
  puller: Addr<Puller>,
}

impl Pusher {
  pub fn new(topic: String) -> Result<Self> {
    let puller = PullerMgr::singleton().get_puller(&topic)?;
    let table = DB.open_table(&topic)?;
    Ok(Pusher { topic, next_offset: AtomicU64::new(Self::init_next_offset(&table)), table, puller })
  }

  pub fn push(&self, req: PushReq) -> Result<(), Error> {
    let offset = self.next_offset.fetch_add(1, Ordering::Relaxed);
    let offset_bytes = <MsgCoder as Coder<u64, Bytes>>::encode_key(offset);
    let value_bytes = Bytes::from(req.value);
    let timestamp = self.table.put_timestamped(offset_bytes, value_bytes.clone())?;
    self.puller.do_send(NotifyMsg { topic: req.topic, offset, value: value_bytes, timestamp });
    Ok(())
  }

  fn init_next_offset(table: &Arc<TtlTable>) -> u64 {
    let table = table.clone().enhance::<u64, Bytes, MsgCoder>();
    if let Some(offset) = table.get_last_key() {
      offset + 1
    } else {
      1
    }
  }
}

#[derive(Clone)]
pub struct PusherWeighter;

impl Weighter<String, Arc<Pusher>> for PusherWeighter {
  fn weight(&self, _key: &String, val: &Arc<Pusher>) -> u32 {
    24 + val.topic.len() as u32
  }
}

pub struct PusherMgr {
  cache: Cache<String, Arc<Pusher>, PusherWeighter>,
}

static PUSHER_MGR: OnceCell<PusherMgr> = OnceCell::new();

impl PusherMgr {
  fn new() -> Self {
    PusherMgr { cache: Cache::with_weighter(10000, 10000 as u64 * 64, PusherWeighter) }
  }

  pub fn singleton() -> &'static Self {
    PUSHER_MGR.get_or_init(|| PusherMgr::new())
  }

  pub fn get_pusher(&self, topic: &String) -> Result<Arc<Pusher>> {
    self.cache.get_or_insert_with(topic, || Ok(Arc::new(Pusher::new(topic.clone())?)))
  }
}
