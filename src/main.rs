#[macro_use]
extern crate serde_derive;

mod config;
mod db;
mod handler;
mod master_client;
mod puller;
mod pusher;
mod registrar;
mod topic_checker;
mod topic_cleaner;

use actix::prelude::*;
use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;

use crate::{config::CONFIG, handler::Handler, registrar::Registrar, topic_cleaner::TopicCleaner};

async fn ws(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
  let resp = ws::WsResponseBuilder::new(Handler::new(), &req, stream)
    .frame_size(CONFIG.server.max_frame_size)
    .start();
  log::info!("http req: {:?}, resp: {:?}", req, resp);
  resp
}

#[actix_web::main]
async fn main() {
  log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

  Registrar::new().start();
  TopicCleaner::new().start();

  HttpServer::new(move || {
    App::new().wrap(middleware::Logger::default()).route("/$ws", web::get().to(ws))
  })
  .backlog(CONFIG.server.backlog)
  .keep_alive(CONFIG.server.keep_alive)
  .max_connection_rate(CONFIG.server.max_connection_rate)
  .max_connections(CONFIG.server.max_connections)
  .workers(CONFIG.server.workers)
  .bind(format!("{}:{}", "0.0.0.0", CONFIG.server.http_port))
  .unwrap()
  .run()
  .await
  .unwrap();
}
