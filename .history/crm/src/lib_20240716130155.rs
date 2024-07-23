mod abi;
mod config;

pub mod pb;

pub use config::AppConfig;

use anyhow::Result;
use crm_metadata::pb::metadata_client::MetadataClient;
use crm_send::pb::notification_client::NotificationClient;
use pb::{
    crm_server::{Crm, CrmServer},
    RecallRequest, RecallResponse, RemindRequest, RemindResponse, WelcomeRequest, WelcomeResponse,
};
use tonic::{
    async_trait, service::interceptor::InterceptedService, transport::Channel, Request, Response,
    Status,
};
use tracing::info;
use user_stat::pb::user_stats_client::UserStatsClient;

use crate::abi::auth;
use user_stat::examples::UserStat;

pub struct CrmService {
    config: AppConfig,
    user_stats: UserStatsClient<Channel>,
    notification: NotificationClient<Channel>,
    metadata: MetadataClient<Channel>,
    sqlx_pool: sqlx::PgPool,
}

#[async_trait]
impl Crm for CrmService {
    async fn welcome(
        &self,
        request: Request<WelcomeRequest>,
    ) -> Result<Response<WelcomeResponse>, Status> {
        let user: &auth::User = request.extensions().get().unwrap();
        let user_stat: Option<UserStat> =
            sqlx::query_as("SELECT * FROM user_stat WHERE gender = 'male'")
                .fetch_optional(&self.sqlx_pool)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .expect("REASON");
        info!("User stat: {}", user_stat);
        info!("User: {:?}", user);
        self.welcome(request.into_inner()).await
    }

    async fn recall(
        &self,
        _request: Request<RecallRequest>,
    ) -> Result<Response<RecallResponse>, Status> {
        todo!()
    }

    async fn remind(
        &self,
        _request: Request<RemindRequest>,
    ) -> Result<Response<RemindResponse>, Status> {
        todo!()
    }
}

impl CrmService {
    pub async fn try_new(config: AppConfig) -> Result<Self> {
        let user_stats = UserStatsClient::connect(config.server.user_stats.clone()).await?;
        let notification = NotificationClient::connect(config.server.notification.clone()).await?;
        let metadata = MetadataClient::connect(config.server.metadata.clone()).await?;
        Ok(Self {
            config,
            user_stats,
            notification,
            metadata,
            sqlx_pool: sqlx::PgPool::connect(
                r#"postgres://postgres:postgres@localhost:5432/stats"#,
            )
            .await?,
        })
    }

    pub fn into_server(
        self,
    ) -> Result<InterceptedService<CrmServer<CrmService>, auth::DecodingKey>> {
        let dk = auth::DecodingKey::load(&self.config.auth.pk)?;
        Ok(CrmServer::with_interceptor(self, dk))
    }
}
