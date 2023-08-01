use std::{
  hash::{Hash, Hasher},
  sync::Arc,
};

use actix::{prelude::*, Actor, Addr, Message as ActixMessage};
use ahash::RandomState as AHasher;
use anyhow::Result;
use bytes::Bytes;
use chrono::prelude::*;
use dashmap::{mapref::entry::Entry as DashEntry, DashMap};
use indexmap::IndexSet;
use maxwell_protocol::{self, *};
use maxwell_utils::prelude::ArbiterPool;
use once_cell::sync::OnceCell;
use seriesdb::prelude::{Coder, Cursor, Db, Table, TtlTable};

use crate::handler::IdAddressMap;
use crate::{
  config::CONFIG,
  db::{MsgCoder, DB},
};

pub struct Puller {
  topic: String,
  table: Arc<TtlTable>,
  last_offset: u64,
  pending_pull_msgs: IndexSet<PullMsg, AHasher>,
}

impl Actor for Puller {
  type Context = Context<Self>;

  fn started(&mut self, _ctx: &mut Self::Context) {
    log::debug!("Puller actor started: topic: {:?}", self.topic);
  }

  fn stopping(&mut self, _: &mut Self::Context) -> Running {
    log::debug!("Puller actor stopping: topic: {:?}", self.topic);
    Running::Stop
  }

  fn stopped(&mut self, _: &mut Self::Context) {
    log::debug!("Puller actor stopped: topic: {:?}", self.topic);
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct PullMsg(pub PullReq);

impl PartialEq for PullMsg {
  fn eq(&self, other: &Self) -> bool {
    self.0.topic == other.0.topic
      && self.0.conn0_ref == other.0.conn0_ref
      && self.0.conn1_ref == other.0.conn1_ref
  }
}
impl Eq for PullMsg {}

impl Hash for PullMsg {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.0.topic.hash(state);
    self.0.conn0_ref.hash(state);
    self.0.conn1_ref.hash(state);
  }
}

impl Handler<PullMsg> for Puller {
  type Result = ();

  #[inline]
  fn handle(&mut self, pull_msg: PullMsg, _ctx: &mut Context<Self>) -> Self::Result {
    log::debug!("pull_msg: {:?}", pull_msg);
    self.pull(pull_msg)
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct NotifyMsg {
  pub topic: String,
  pub offset: u64,
  pub value: Bytes,
  pub timestamp: u32,
}

impl Handler<NotifyMsg> for Puller {
  type Result = ();

  #[inline]
  fn handle(&mut self, notify_msg: NotifyMsg, _ctx: &mut Context<Self>) -> Self::Result {
    log::debug!("notify_msg: {:?}", notify_msg);
    self.notify_all(notify_msg)
  }
}

impl Puller {
  #[inline]
  pub fn new(topic: String) -> Result<Self> {
    let table = DB.open_table(&topic)?;
    let last_offset = Self::recover_last_offset(&table);
    Ok(Self { topic, table, last_offset, pending_pull_msgs: IndexSet::with_hasher(AHasher::new()) })
  }

  #[inline]
  pub fn start(topic: String) -> Result<Addr<Self>> {
    let puller = Puller::new(topic)?;
    Ok(Puller::start_in_arbiter(&ArbiterPool::singleton().fetch_arbiter(), move |_ctx| puller))
  }

  fn pull(&mut self, mut pull_msg: PullMsg) {
    let pull_req = &mut pull_msg.0;
    if pull_req.offset >= 0 {
      let adjusted_offset = self.adjust_offset(pull_req.offset as u64);
      if adjusted_offset > self.last_offset {
        pull_req.offset = adjusted_offset as i64;
        self.add_to_pendings(pull_msg);
      } else {
        let msgs = self.get_since_by_offset(adjusted_offset, pull_req.limit);
        if msgs.len() > 0 {
          Self::notify_one(&pull_req, msgs);
          self.remove_from_pendings(&pull_msg);
        } else {
          pull_req.offset = adjusted_offset as i64;
          self.add_to_pendings(pull_msg);
        }
      }
    } else {
      let seconds_elapsed = Self::adjust_seconds_elapsed(pull_req.offset.abs() as u64);
      let msgs = self.get_until_by_time(seconds_elapsed, pull_req.limit);
      if msgs.len() > 0 {
        Self::notify_one(&pull_req, msgs);
        self.remove_from_pendings(&pull_msg);
      } else {
        pull_req.offset = (self.last_offset + 1) as i64;
        self.add_to_pendings(pull_msg);
      }
    }
  }

  fn get_since_by_offset(&self, offset: u64, limit: u32) -> Vec<Msg> {
    let mut values = Vec::<Msg>::new();
    let mut count = 0;
    let mut cursor = self.table.new_cursor();
    cursor.seek(<MsgCoder as Coder<u64, Bytes>>::encode_key(offset));
    while cursor.is_valid() {
      if count >= limit {
        break;
      }
      let curr_offset = <MsgCoder as Coder<u64, Bytes>>::decode_key(cursor.key().unwrap());
      let (timestamp, encoded_value) = cursor.timestamped_value().unwrap();
      let value = <MsgCoder as Coder<u64, Bytes>>::decode_value(encoded_value);
      values.push(Msg { offset: curr_offset, value: value.into(), timestamp: timestamp as u64 });
      cursor.next();
      count += 1;
    }
    values
  }

  fn get_until_by_time(&self, seconds_elapsed: u16, limit: u32) -> Vec<Msg> {
    let mut values = Vec::<Msg>::new();
    let mut count = 0;
    let mut cursor = self.table.new_cursor();
    cursor.seek(<MsgCoder as Coder<u64, Bytes>>::encode_key(self.last_offset));
    while cursor.is_valid() {
      if count >= limit {
        break;
      }
      let curr_offset = <MsgCoder as Coder<u64, Bytes>>::decode_key(cursor.key().unwrap());
      let (timestamp, encoded_value) = cursor.timestamped_value().unwrap();
      if (timestamp + seconds_elapsed as u32) < (Utc::now().timestamp() as u32) {
        break;
      }
      let value = <MsgCoder as Coder<u64, Bytes>>::decode_value(encoded_value);
      values.push(Msg { offset: curr_offset, value: value.into(), timestamp: timestamp as u64 });
      cursor.prev();
      count += 1;
    }
    values.reverse();
    values
  }

  fn notify_one(pull_req: &PullReq, msgs: Vec<Msg>) {
    if let Some(addr) = IdAddressMap::singleton().get(pull_req.conn1_ref) {
      addr.do_send(
        PullRep {
          msgs,
          conn0_ref: pull_req.conn0_ref,
          conn1_ref: pull_req.conn1_ref,
          r#ref: pull_req.r#ref,
        }
        .into_enum(),
      )
    }
  }

  fn notify_all(&mut self, notify_msg: NotifyMsg) {
    self.update_last_offset(notify_msg.offset);

    let mut notified_pull_reqs: Vec<usize> = vec![];
    let len = self.pending_pull_msgs.len();
    for i in 0..len {
      if let Some(pull_msg) = { self.pending_pull_msgs.get_index(i) } {
        let pull_req: &PullReq = &pull_msg.0;
        if notify_msg.offset == pull_req.offset as u64 {
          Self::notify_one(
            pull_req,
            vec![Msg {
              offset: notify_msg.offset,
              value: notify_msg.value.clone().into(),
              timestamp: notify_msg.timestamp as u64,
            }],
          );
          notified_pull_reqs.push(i);
        } else {
          let msgs = self.get_since_by_offset(pull_req.offset as u64, pull_req.limit);
          if msgs.len() > 0 {
            Self::notify_one(pull_req, msgs);
            notified_pull_reqs.push(i);
          }
        }
      }
    }
    for i in notified_pull_reqs {
      self.pending_pull_msgs.swap_remove_index(i);
    }
  }

  #[inline]
  fn add_to_pendings(&mut self, pull_msg: PullMsg) {
    self.pending_pull_msgs.replace(pull_msg);
  }

  #[inline]
  fn remove_from_pendings(&mut self, pull_msg: &PullMsg) {
    self.pending_pull_msgs.remove(pull_msg);
  }

  #[inline]
  fn adjust_offset(&self, offset: u64) -> u64 {
    if offset as u64 + CONFIG.puller.max_offset_dif < self.last_offset {
      self.last_offset - CONFIG.puller.max_offset_dif
    } else {
      offset as u64
    }
  }

  #[inline]
  fn adjust_seconds_elapsed(seconds_elapsed: u64) -> u16 {
    if seconds_elapsed > 300 {
      300
    } else {
      seconds_elapsed as u16
    }
  }

  #[inline]
  fn recover_last_offset(table: &Arc<TtlTable>) -> u64 {
    let table = table.clone().enhance::<u64, Bytes, MsgCoder>();
    if let Some(offset) = table.get_last_key() {
      offset
    } else {
      0
    }
  }

  #[inline]
  fn update_last_offset(&mut self, last_offset: u64) {
    if self.last_offset < last_offset {
      self.last_offset = last_offset;
    }
  }
}

pub struct PullerMgr {
  pullers: DashMap<String, Addr<Puller>, AHasher>,
}

static PULLER_MGR: OnceCell<PullerMgr> = OnceCell::new();

impl PullerMgr {
  fn new() -> Self {
    PullerMgr { pullers: DashMap::with_capacity_and_hasher(1000, AHasher::default()) }
  }

  pub fn singleton() -> &'static Self {
    PULLER_MGR.get_or_init(|| PullerMgr::new())
  }

  pub fn get_puller(&self, topic: &String) -> Result<Addr<Puller>> {
    match self.pullers.entry(topic.clone()) {
      DashEntry::Occupied(mut occupied) => {
        let puller = occupied.get();
        if puller.connected() {
          Ok(puller.clone())
        } else {
          let puller = Puller::start(occupied.key().clone())?;
          occupied.insert(puller.clone());
          Ok(puller)
        }
      }
      DashEntry::Vacant(vacant) => {
        let puller = Puller::start(vacant.key().clone())?;
        vacant.insert(puller.clone());
        Ok(puller)
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// test cases
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
  use std::time::Duration;

  use tokio::time::sleep;

  use super::*;

  #[actix::test]
  async fn test_send_msg() {
    {
      let puller = Puller::start("huobi:btcusdt.1m".to_owned()).unwrap();
      for _ in 1..2 {
        let pull_req = maxwell_protocol::PullReq {
          topic: "huobi:btcusdt.1m".to_owned(),
          offset: 10,
          limit: 10,
          conn0_ref: 10,
          conn1_ref: 10,
          r#ref: 1,
        };
        let res = puller.send(PullMsg(pull_req)).await;
        println!("result: {:?}", res);
      }
    }
    sleep(Duration::from_millis(200)).await;
  }
}
