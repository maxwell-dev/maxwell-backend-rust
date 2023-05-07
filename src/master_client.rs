use std::time::Duration;

use actix::prelude::{Addr, Recipient};
use maxwell_protocol::{ProtocolMsg, SendError};
use maxwell_utils::prelude::{
  ConnectionFull, ConnectionOptions, ConnectionStatusChangedMsg, SubscribeConnectionStatusMsg,
  TimeoutExt, UnsubscribeConnectionStatusMsg,
};
use once_cell::sync::Lazy;

use crate::config::CONFIG;

pub struct MasterClient {
  _endpoints: Vec<String>,
  connection: Addr<ConnectionFull>,
}

impl MasterClient {
  pub fn new(endpoints: &Vec<String>) -> Self {
    let connection = ConnectionFull::start2(
      endpoints[0].clone(),
      ConnectionOptions { reconnect_delay: 1000, ping_interval: None },
    );
    MasterClient { _endpoints: endpoints.clone(), connection }
  }

  pub async fn send(&self, msg: ProtocolMsg) -> Result<ProtocolMsg, SendError> {
    self.connection.send(msg).timeout_ext(Duration::from_secs(5)).await
  }

  pub fn subscribe_connection_status_changed(&self, r: Recipient<ConnectionStatusChangedMsg>) {
    self.connection.do_send(SubscribeConnectionStatusMsg(r))
  }

  pub fn unsubscribe_connection_status_changed(&self, r: Recipient<ConnectionStatusChangedMsg>) {
    self.connection.do_send(UnsubscribeConnectionStatusMsg(r))
  }
}

pub static MASTER_CLIENT: Lazy<MasterClient> =
  Lazy::new(|| MasterClient::new(&CONFIG.master_endpoints));

#[cfg(test)]
mod tests {
  #[actix::test]
  async fn test_send() {}
}
