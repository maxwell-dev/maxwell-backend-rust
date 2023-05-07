#[macro_use]
extern crate serde_derive;

mod config;
mod db;
mod handler;
mod master_client;
mod puller;
mod pusher;
mod registrar;

use actix::prelude::*;
use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;

use crate::config::CONFIG;
use crate::{handler::Handler, registrar::Registrar};

async fn index(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
  let resp = ws::start(Handler::new(), &req, stream);
  resp
}

#[actix_web::main]
async fn main() {
  log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

  let registrar = Registrar::new();
  registrar.start();

  HttpServer::new(move || {
    App::new().wrap(middleware::Logger::default()).route("/ws", web::get().to(index))
  })
  .bind(format!("{}:{}", "0.0.0.0", CONFIG.http_port))
  .unwrap()
  .run()
  .await
  .unwrap();
}
