use std::{
  hash::{BuildHasher, Hash, Hasher},
  rc::Rc,
  sync::Arc,
};

use actix::{prelude::*, Actor, Addr, Message as ActixMessage};
use ahash::{AHashMap, RandomState as AHasher};
use bytes::Bytes;
use dashmap::{mapref::entry::Entry as DashEntry, DashMap};
use indexmap::IndexSet;
use maxwell_protocol::{self, *};
use maxwell_utils::prelude::ArbiterPool;
use once_cell::sync::OnceCell;
use seriesdb::prelude::{Coder, Cursor, Db, Error as SeriesdbError, Table, TtlTable};

use crate::handler::IdAddressMap;
use crate::{
  config::CONFIG,
  db::{MsgCoder, DB},
};

pub struct Puller {
  id: u16,
  pending_pull_reqs: AHashMap<String, IndexSet<Rc<PullMsg>, AHasher>>,
}

impl Actor for Puller {
  type Context = Context<Self>;

  fn started(&mut self, _ctx: &mut Self::Context) {
    log::info!("Puller actor started: id: {:?}", self.id);
  }

  fn stopping(&mut self, _: &mut Self::Context) -> Running {
    log::info!("Puller actor stopping: id: {:?}", self.id);
    Running::Stop
  }

  fn stopped(&mut self, _: &mut Self::Context) {
    log::info!("Puller actor stopped: id: {:?}", self.id);
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
    self.pull(pull_msg)
  }
}

#[derive(Debug, ActixMessage)]
#[rtype(result = "()")]
pub struct NotifyMsg {
  pub topic: String,
  pub offset: u64,
  pub value: Bytes,
}

impl Handler<NotifyMsg> for Puller {
  type Result = ();

  #[inline]
  fn handle(&mut self, notify_msg: NotifyMsg, _ctx: &mut Context<Self>) -> Self::Result {
    log::info!("notify_msg: {:?}", notify_msg);
    self.notify(notify_msg)
  }
}

impl Puller {
  #[inline]
  pub fn new(id: u16) -> Self {
    Self { id, pending_pull_reqs: AHashMap::with_capacity(1024) }
  }

  #[inline]
  pub fn start(id: u16) -> Addr<Self> {
    Puller::start_in_arbiter(&ArbiterPool::singleton().fetch_arbiter(), move |_ctx| Puller::new(id))
  }

  // @TODO negative offset
  fn pull(&mut self, pull_msg: PullMsg) {
    let pull_req = &pull_msg.0;
    match DB.open_table(&pull_req.topic) {
      Ok(table) => {
        let msgs = Self::get_since(&table, pull_req.offset as u64, pull_req.limit);
        if msgs.len() > 0 {
          Self::notify_one(&pull_req, msgs);
          self.remove_from_pendings(&pull_msg);
        } else {
          self.add_to_pendings(pull_msg);
        }
      }
      Err(err) => {
        Self::notify_err(&pull_req, err);
      }
    }
  }

  fn get_since(table: &Arc<TtlTable>, key: u64, limit: u32) -> Vec<Msg> {
    let mut values = Vec::<Msg>::new();
    let mut count = 0;
    let mut cursor = table.new_cursor();
    cursor.seek(<MsgCoder as Coder<u64, Bytes>>::encode_key(key));
    while cursor.is_valid() {
      if count >= limit {
        break;
      }
      let key = <MsgCoder as Coder<u64, Bytes>>::decode_key(cursor.key().unwrap());
      let value = <MsgCoder as Coder<u64, Bytes>>::decode_value(cursor.value().unwrap());
      values.push(Msg { offset: key, value: value.into(), timestamp: 0 });
      cursor.next();
      count += 1;
    }
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

  fn notify_err(pull_req: &PullReq, err: SeriesdbError) {
    if let Some(addr) = IdAddressMap::singleton().get(pull_req.conn1_ref) {
      addr.do_send(
        maxwell_protocol::Error2Rep {
          code: 1,
          desc: format!("Failed to pull: err: {:?}", err),
          conn0_ref: pull_req.conn0_ref,
          conn1_ref: pull_req.conn1_ref,
          r#ref: pull_req.r#ref,
        }
        .into_enum(),
      )
    }
  }

  fn notify(&mut self, notify_msg: NotifyMsg) {
    if let Some(pendings) = self.pending_pull_reqs.get_mut(&notify_msg.topic) {
      let len = { pendings.len() };
      for i in (0..len).rev() {
        if let Some(pull_msg) = { pendings.get_index(i).cloned() } {
          if notify_msg.offset - 1 == pull_msg.0.offset as u64 {
            Self::notify_one(
              &pull_msg.0,
              vec![Msg {
                offset: notify_msg.offset,
                value: notify_msg.value.clone().into(),
                timestamp: 0,
              }],
            );
            pendings.remove(&pull_msg);
          } else {
            match DB.open_table(&pull_msg.0.topic) {
              Ok(table) => {
                let msgs = Self::get_since(&table, pull_msg.0.offset as u64, pull_msg.0.limit);
                if msgs.len() > 0 {
                  Self::notify_one(&pull_msg.0, msgs);
                  pendings.remove(&pull_msg);
                }
              }
              Err(err) => {
                log::warn!("Failed to open table: topic: {:?}, err: {:?}", &pull_msg.0.topic, err);
                pendings.remove(&pull_msg);
              }
            }
          }
        }
      }
    }
  }

  fn add_to_pendings(&mut self, pull_msg: PullMsg) {
    if let Some(pendings) = self.pending_pull_reqs.get_mut(&pull_msg.0.topic) {
      pendings.insert(Rc::new(pull_msg));
    } else {
      let topic = pull_msg.0.topic.clone();
      let mut pendings = IndexSet::with_hasher(AHasher::new());
      pendings.insert(Rc::new(pull_msg));
      self.pending_pull_reqs.insert(topic, pendings);
    }
  }

  fn remove_from_pendings(&mut self, pull_msg: &PullMsg) {
    self
      .pending_pull_reqs
      .get_mut(&pull_msg.0.topic)
      .and_then(|pendings| Some(pendings.remove(pull_msg)));
  }
}

pub struct PullerMgr {
  pullers: DashMap<u16, Addr<Puller>, AHasher>,
  hash_builder: AHasher,
}

static PULLER_MGR: OnceCell<PullerMgr> = OnceCell::new();

impl PullerMgr {
  fn new() -> Self {
    PullerMgr {
      pullers: DashMap::with_capacity_and_hasher(CONFIG.puller_number as usize, AHasher::default()),
      hash_builder: AHasher::default(),
    }
  }

  pub fn singleton() -> &'static Self {
    PULLER_MGR.get_or_init(|| PullerMgr::new())
  }

  pub fn get_puller(&self, topic: &String) -> Addr<Puller> {
    let id = self.get_id(topic);
    match self.pullers.entry(id) {
      DashEntry::Occupied(mut occupied) => {
        let puller = occupied.get();
        if puller.connected() {
          puller.clone()
        } else {
          let puller = Puller::start(id);
          occupied.insert(puller.clone());
          puller
        }
      }
      DashEntry::Vacant(vacant) => {
        let puller = Puller::start(id);
        vacant.insert(puller.clone());
        puller
      }
    }
  }

  fn get_id(&self, topic: &String) -> u16 {
    let hash = self.gen_hash(topic);
    hash.div_euclid(CONFIG.puller_number as u64) as u16
  }

  #[inline]
  fn gen_hash(&self, topic: &String) -> u64 {
    let mut hasher = self.hash_builder.build_hasher();
    topic.hash(&mut hasher);
    hasher.finish()
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
      let puller = Puller::start(1);
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
