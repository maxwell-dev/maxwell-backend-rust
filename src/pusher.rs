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

use crate::{config::CONFIG, db::MsgCoder, db::DB, puller::NotifyMsg, puller::PullerMgr};

pub struct Pusher {
  _topic: String,
  next_offset: AtomicU64,
  table: Arc<TtlTable>,
}

impl Pusher {
  #[inline]
  pub fn new(topic: String) -> Result<Self> {
    let table = DB.open_table(&topic)?;
    Ok(Pusher { _topic: topic, next_offset: AtomicU64::new(Self::init_next_offset(&table)), table })
  }

  #[inline]
  pub fn push(&self, req: PushReq) -> Result<(), Error> {
    let offset = self.next_offset.fetch_add(1, Ordering::Relaxed);
    let offset_bytes = <MsgCoder as Coder<u64, Bytes>>::encode_key(offset);
    let value_bytes = Bytes::from(req.value);
    let timestamp = self.table.put_timestamped(offset_bytes, value_bytes.clone())?;
    if let Some(puller) = PullerMgr::singleton().get_puller(&req.topic) {
      puller.do_send(NotifyMsg { topic: req.topic, offset, value: value_bytes, timestamp });
    }
    Ok(())
  }

  #[inline]
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
  #[inline]
  fn new() -> Self {
    PusherMgr {
      pushers: DashMap::with_capacity_and_hasher(
        CONFIG.pusher.pusher_mgr_capacity as usize,
        AHasher::default(),
      ),
    }
  }

  #[inline]
  pub fn singleton() -> &'static Self {
    PUSHER_MGR.get_or_init(|| PusherMgr::new())
  }

  #[inline]
  pub fn get_pusher(&self, topic: &String) -> Option<Arc<Pusher>> {
    if let Some(puller) = self.pushers.get(topic) {
      Some(puller.clone())
    } else {
      None
    }
  }

  #[inline]
  pub fn get_or_new_pusher(&self, topic: &String) -> Result<Arc<Pusher>> {
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

  #[inline]
  pub fn clear(&self) {
    self.pushers.clear();
  }
}
