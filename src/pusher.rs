use std::sync::{
  atomic::{AtomicU64, Ordering},
  Arc,
};

use ahash::RandomState as AHasher;
use anyhow::{Error, Result};
use bytes::Bytes;
use dashmap::{mapref::entry::Entry as DashEntry, DashMap};
use maxwell_protocol::PushReq;
use once_cell::sync::OnceCell;
use seriesdb::{
  prelude::{Coder, Db, Table},
  table::TtlTable,
};

use crate::{db::MsgCoder, puller::NotifyMsg};
use crate::{db::DB, puller::PullerMgr};

pub struct Pusher {
  _topic: String,
  next_offset: AtomicU64,
  table: Arc<TtlTable>,
}

impl Pusher {
  pub fn new(topic: String) -> Result<Self> {
    let table = DB.open_table(&topic)?;
    Ok(Pusher { _topic: topic, next_offset: AtomicU64::new(Self::init_next_offset(&table)), table })
  }

  pub fn push(&self, req: PushReq) -> Result<(), Error> {
    let offset = self.next_offset.fetch_add(1, Ordering::Relaxed);
    let offset_bytes = <MsgCoder as Coder<u64, Bytes>>::encode_key(offset);
    let value_bytes = Bytes::from(req.value);
    let timestamp = self.table.put_timestamped(offset_bytes, value_bytes.clone())?;
    let puller = PullerMgr::singleton().get_puller(&req.topic)?;
    puller.do_send(NotifyMsg { topic: req.topic, offset, value: value_bytes, timestamp });
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

pub struct PusherMgr {
  pushers: DashMap<String, Arc<Pusher>, AHasher>,
}

static PUSHER_MGR: OnceCell<PusherMgr> = OnceCell::new();

impl PusherMgr {
  fn new() -> Self {
    PusherMgr { pushers: DashMap::with_capacity_and_hasher(10000, AHasher::default()) }
  }

  pub fn singleton() -> &'static Self {
    PUSHER_MGR.get_or_init(|| PusherMgr::new())
  }

  pub fn get_pusher(&self, topic: &String) -> Result<Arc<Pusher>> {
    match self.pushers.entry(topic.clone()) {
      DashEntry::Occupied(occupied) => {
        let pusher = occupied.get();
        Ok(Arc::clone(pusher))
      }
      DashEntry::Vacant(vacant) => {
        let pusher = Arc::new(Pusher::new(topic.clone())?);
        vacant.insert(Arc::clone(&pusher));
        Ok(pusher)
      }
    }
  }
}
