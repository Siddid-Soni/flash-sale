//! The fake payment provider, a service of its own (the stand-in for Stripe).
//! It serves
//!
//! - the payment page the user pays at, `/fake-pay/{session_id}` and its pay /
//!   cancel forms (`flash_sale::payment::pay_page`), which the gateway proxies;
//! - a small Stripe-like HTTP API the order processor and the expiration worker
//!   call: create a session (with an `Idempotency-Key`), expire it, refund it
//!   (`flash_sale::payment::provider_api`), on the compose network only;
//!
//! and, in the background, delivers the signed webhooks the payment page
//! queues, with retries, to `PAYMENT_WEBHOOK_URL`
//! (`flash_sale::payment::dispatcher`). It owns the `fakepay:*` Redis keys:
//! nothing else touches them.
//!
//! Usage: `cargo run --bin fake_payment_provider`

use actix_web::{middleware, web, App, HttpServer};

use flash_sale::{
    cache,
    config::Config,
    payment::{
        self,
        dispatcher::{Dispatcher, DispatcherSettings},
        FakeProvider,
    },
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let config = Config::from_env();
    let redis = cache::connect(&config).await;

    let provider = web::Data::new(FakeProvider::new(
        redis,
        config.payment_page_base_url.clone(),
        config.payment_webhook_secret.clone(),
    ));
    // The dispatcher's client is not `Send`, so it runs on this runtime
    let dispatcher = Dispatcher::new(provider.clone().into_inner(), config.payment_webhook_url.clone(), DispatcherSettings::default());
    actix_web::rt::spawn(dispatcher.run());

    let bind_addr = config.provider_bind_addr.clone();
    let config = web::Data::new(config);
    log::info!("starting the fake payment provider at http://{bind_addr}");
    HttpServer::new(move || {
        App::new()
            .app_data(provider.clone())
            .app_data(config.clone())
            .app_data(web::JsonConfig::default().error_handler(payment::provider_api::json_error))
            .wrap(middleware::Logger::default())
            .configure(payment::pay_page::configure)
            .configure(payment::provider_api::configure)
    })
    .bind(bind_addr)?
    .run()
    .await
}
