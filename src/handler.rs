use std::{
  cell::RefCell,
  fmt::{Display, Error as StdError, Formatter},
  rc::Rc,
  sync::atomic::{AtomicU32, Ordering},
};

use actix::{prelude::*, Actor};
use actix_web_actors::ws;
use ahash::RandomState as AHasher;
use anyhow::Result;
use dashmap::DashMap;
use maxwell_protocol::{self, *};
use once_cell::sync::OnceCell;
use thiserror::Error as ThisError;

use crate::{
  puller::{PullMsg, PullerMgr},
  pusher::PusherMgr,
  topic_checker::TOPIC_CHECKER,
};

static ID_SEED: AtomicU32 = AtomicU32::new(1);

#[inline]
fn next_id() -> u32 {
  ID_SEED.fetch_add(1, Ordering::Relaxed)
}

static ID_ADDRESS_MAP: OnceCell<IdAddressMap> = OnceCell::new();

#[derive(Debug)]
pub struct IdAddressMap(DashMap<u32, Recipient<ProtocolMsg>, AHasher>);

impl IdAddressMap {
  #[inline]
  pub fn new() -> Self {
    IdAddressMap(DashMap::with_capacity_and_hasher(1024, AHasher::default()))
  }

  #[inline]
  pub fn singleton() -> &'static Self {
    ID_ADDRESS_MAP.get_or_init(|| Self::new())
  }

  #[inline]
  pub fn add(&self, id: u32, address: Recipient<ProtocolMsg>) {
    self.0.insert(id, address);
  }

  #[inline]
  pub fn remove(&self, id: u32) {
    self.0.remove(&id);
  }

  #[inline]
  pub fn get(&self, id: u32) -> Option<Recipient<ProtocolMsg>> {
    if let Some(address) = self.0.get(&id) {
      Some(address.clone())
    } else {
      None
    }
  }
}

#[derive(ThisError, Debug)]
struct UnknownTopicError {
  topic: String,
}
impl Display for UnknownTopicError {
  fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), StdError> {
    write!(fmt, "Unknown topic: {}", self.topic)
  }
}

#[derive(Debug, Clone)]
struct HandlerInner {
  id: u32,
  recipient: RefCell<Option<Recipient<ProtocolMsg>>>,
  id_address_map: &'static IdAddressMap,
}

impl HandlerInner {
  fn new() -> Self {
    HandlerInner {
      id: next_id(),
      recipient: RefCell::new(None),
      id_address_map: IdAddressMap::singleton(),
    }
  }

  fn assign_address(&self, address: Recipient<ProtocolMsg>) {
    *self.recipient.borrow_mut() = Some(address.clone());
    self.id_address_map.add(self.id, address);
  }

  async fn handle_external_msg(&self, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::debug!("received external msg: {:?}", protocol_msg);
    match protocol_msg {
      ProtocolMsg::PingReq(req) => maxwell_protocol::PingRep { r#ref: req.r#ref }.into_enum(),
      ProtocolMsg::PushReq(req) => {
        let r#ref = req.r#ref;
        match self.try_handle_push_msg(req).await {
          Ok(_) => maxwell_protocol::PushRep { r#ref }.into_enum(),
          Err(err) => {
            if let Some(down_err) = err.downcast_ref::<UnknownTopicError>() {
              log::error!("Pushing to unknown topic: {}", down_err.topic);
              maxwell_protocol::ErrorRep {
                code: ErrorCode::UnknownTopic as i32,
                desc: format!("Unknown topic: {}", down_err.topic),
                r#ref,
              }
              .into_enum()
            } else {
              log::error!("Failed to push: err: {:?}", err);
              maxwell_protocol::ErrorRep {
                code: ErrorCode::FailedToPush as i32,
                desc: format!("Failed to push: err: {:?}", err),
                r#ref,
              }
              .into_enum()
            }
          }
        }
      }
      ProtocolMsg::PullReq(mut req) => {
        req.conn1_ref = self.id;
        let conn0_ref = req.conn0_ref;
        let r#ref = req.r#ref;
        match self.try_handle_pull_msg(req).await {
          Ok(_) => maxwell_protocol::ProtocolMsg::None,
          Err(err) => {
            if let Some(down_err) = err.downcast_ref::<UnknownTopicError>() {
              log::error!("Pulling from unknown topic: {}", down_err.topic);
              maxwell_protocol::Error2Rep {
                code: ErrorCode::UnknownTopic as i32,
                desc: format!("Unknown topic: {}", down_err.topic),
                conn0_ref: conn0_ref,
                conn1_ref: self.id,
                r#ref,
              }
              .into_enum()
            } else {
              log::error!("Failed to pull: err: {:?}", err);
              maxwell_protocol::Error2Rep {
                code: ErrorCode::FailedToPull as i32,
                desc: format!("Failed to pull: err: {:?}", err),
                conn0_ref: conn0_ref,
                conn1_ref: self.id,
                r#ref,
              }
              .into_enum()
            }
          }
        }
      }
      other => {
        log::error!("Received unknown msg: {:?}", other);
        maxwell_protocol::ErrorRep {
          code: ErrorCode::UnknownMsg as i32,
          desc: format!("Received unknown msg: {:?}", other),
          r#ref: maxwell_protocol::get_ref(&other),
        }
        .into_enum()
      }
    }
  }

  async fn handle_internal_msg(&self, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::debug!("received internal msg: {:?}", protocol_msg);
    match protocol_msg {
      ProtocolMsg::PullRep(_) => protocol_msg,
      other => maxwell_protocol::ErrorRep {
        code: ErrorCode::UnknownMsg as i32,
        desc: format!("Received unknown msg: {:?}", other),
        r#ref: maxwell_protocol::get_ref(&other),
      }
      .into_enum(),
    }
  }

  #[inline]
  async fn try_handle_push_msg(&self, req: PushReq) -> Result<()> {
    if let Some(pusher) = PusherMgr::singleton().get_pusher(&req.topic) {
      pusher.push(req)?;
      Ok(())
    } else {
      if TOPIC_CHECKER.check(&req.topic).await? {
        let pusher = PusherMgr::singleton().get_or_new_pusher(&req.topic)?;
        pusher.push(req)?;
        Ok(())
      } else {
        Err(UnknownTopicError { topic: req.topic.clone() }.into())
      }
    }
  }

  #[inline]
  async fn try_handle_pull_msg(&self, req: PullReq) -> Result<()> {
    if let Some(puller) = PullerMgr::singleton().get_puller(&req.topic) {
      puller.try_send(PullMsg(req))?;
      Ok(())
    } else {
      if TOPIC_CHECKER.check(&req.topic).await? {
        let puller = PullerMgr::singleton().get_or_new_puller(&req.topic)?;
        puller.try_send(PullMsg(req))?;
        Ok(())
      } else {
        Err(UnknownTopicError { topic: req.topic.clone() }.into())
      }
    }
  }
}

#[derive(Debug)]
pub struct Handler {
  inner: Rc<HandlerInner>,
}

impl Actor for Handler {
  type Context = ws::WebsocketContext<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    log::debug!("Handler actor started: id: {:?}", self.inner.id);
    let address = ctx.address().recipient();
    self.inner.id_address_map.add(self.inner.id, address.clone());
    self.inner.assign_address(address);
  }

  fn stopping(&mut self, _: &mut Self::Context) -> Running {
    log::debug!("Handler actor stopping: id: {:?}", self.inner.id);
    self.inner.id_address_map.remove(self.inner.id);
    Running::Stop
  }

  fn stopped(&mut self, _: &mut Self::Context) {
    log::debug!("Handler actor stopped: id: {:?}", self.inner.id);
  }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for Handler {
  fn handle(&mut self, ws_msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
    match ws_msg {
      Ok(ws::Message::Ping(ws_msg)) => {
        ctx.pong(&ws_msg);
      }
      Ok(ws::Message::Pong(_)) => (),
      Ok(ws::Message::Text(_)) => (),
      Ok(ws::Message::Binary(bin)) => {
        let inner = self.inner.clone();
        async move {
          let res = maxwell_protocol::decode(&bin.into());
          match res {
            Ok(req) => Ok(inner.handle_external_msg(req).await),
            Err(err) => Err(err),
          }
        }
        .into_actor(self)
        .map(move |res, _act, ctx| match res {
          Ok(msg) => {
            if msg.is_some() {
              ctx.binary(maxwell_protocol::encode(&msg));
            }
          }
          Err(err) => log::error!("Failed to decode msg: {:?}", err),
        })
        .spawn(ctx)
      }
      Ok(ws::Message::Close(_)) => ctx.stop(),
      _ => log::error!("Received unknown msg: {:?}", ws_msg),
    }
  }
}

impl actix::Handler<ProtocolMsg> for Handler {
  type Result = Result<ProtocolMsg, HandleError<ProtocolMsg>>;

  fn handle(&mut self, protocol_msg: ProtocolMsg, ctx: &mut Self::Context) -> Self::Result {
    let inner = self.inner.clone();
    async move { inner.handle_internal_msg(protocol_msg).await }
      .into_actor(self)
      .map(move |res, _act, ctx| {
        if res.is_some() {
          ctx.binary(maxwell_protocol::encode(&res));
        }
      })
      .spawn(ctx);
    Ok(ProtocolMsg::None)
  }
}

impl Handler {
  pub fn new() -> Self {
    Self { inner: Rc::new(HandlerInner::new()) }
  }
}
