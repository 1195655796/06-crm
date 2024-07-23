use std::mem;

use anyhow::Result;
use crm::{AppConfig, CrmService};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{error, info, Level};
use tracing_subscriber::{
    filter::LevelFilter,
    fmt::{format::FmtSpan, Layer},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    Layer as _,
};

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::registry()
        .with(
            Layer::new()
                .with_max_level(Level::DEBUG)
                .with_target(true)
                .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT),
        )
        .with(tracing_subscriber::fmt::layer().with_env_filter("my_app=debug"));

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global subscriber");

    let mut config = AppConfig::load().expect("Failed to load config");

    let tls = mem::take(&mut config.server.tls);

    let addr = config.server.port;
    let addr = format!("[::1]:{}", addr).parse().unwrap();
    info!("CRM service listening on {}", addr);
    let svc = match CrmService::try_new(config).await {
        Ok(svc) => svc.into_server()?,
        Err(err) => {
            error!("Failed to create CRM service: {}", err);
            return Err(err.into());
        }
    };

    if let Some(tls) = tls {
        let identity = Identity::from_pem(tls.cert, tls.key);
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))?
            .add_service(svc)
            .serve(addr)
            .await?;
    } else {
        Server::builder().add_service(svc).serve(addr).await?;
    }
    Ok(())
}