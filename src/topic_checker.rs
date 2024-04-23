use anyhow::{Error, Result};
use maxwell_protocol::{self, *};
use once_cell::sync::Lazy;
use quick_cache::sync::Cache;

// use tokio::sync::OnceCell;
use crate::{config::CONFIG, master_client::MASTER_CLIENT};

// static THIS_ENDPOINT: OnceCell<String> = OnceCell::const_new();
pub static TOPIC_CHECKER: Lazy<TopicChecker> = Lazy::new(|| TopicChecker::new());

pub struct TopicChecker {
  cache: Cache<String, bool>,
}

impl TopicChecker {
  #[inline]
  pub fn new() -> Self {
    Self { cache: Cache::new(CONFIG.topic_checker.cache_size as usize) }
  }

  pub async fn check(&self, topic: &String) -> Result<bool> {
    // let this_endpoint = THIS_ENDPOINT
    //   .get_or_try_init(|| async {
    //     match MASTER_CLIENT.send(ResolveIpReq { r#ref: 0 }.into_enum()).await {
    //       Ok(rep) => match rep {
    //         ProtocolMsg::ResolveIpRep(rep) => Ok(format!("{}:{}", rep.ip, CONFIG.server.http_port)),
    //         err => Err(Error::msg(format!("Failed to resolve ip: err: {:?}", err))),
    //       },
    //       Err(err) => Err(Error::msg(format!("Failed to resolve ip: err: {:?}", err))),
    //     }
    //   })
    //   .await?;

    self
      .cache
      .get_or_insert_async(topic, async {
        match MASTER_CLIENT
          .send(LocateTopicReq { topic: topic.clone(), r#ref: 0 }.into_enum())
          .await
        {
          Ok(rep) => match rep {
            ProtocolMsg::LocateTopicRep(_rep) => {
              // if &rep.endpoint == this_endpoint {
              //   Ok(true)
              // } else {
              //   Ok(false)
              // }
              Ok(true)
            }
            err => Err(Error::msg(format!("Failed to check topic: {:?}, err: {:?}", topic, err))),
          },
          Err(err) => {
            Err(Error::msg(format!("Failed to check topic: {:?}, err: {:?}", topic, err)))
          }
        }
      })
      .await
  }

  #[inline]
  pub fn clear(&self) {
    self.cache.clear();
  }
}
