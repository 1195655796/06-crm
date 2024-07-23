pub mod auth;

use crate::{
    pb::{WelcomeRequest, WelcomeResponse,RemindRequest,RemindResponse},
    CrmService,
};
use chrono::{Duration, Utc};
use crm_metadata::pb::{Content, MaterializeRequest};
use crm_send::pb::SendRequest;
use futures::StreamExt;
use sqlx;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::{Response, Status};
use tracing::info;
use tracing::warn;
use user_stat::examples::gen::gender;
use user_stat::examples::UserStat;
use user_stat::pb::QueryRequest;
use rand::seq::SliceRandom;
use rand::seq::IteratorRandom;
use jwt_simple::reexports::rand;
use std::process::id;


impl CrmService {
    pub async fn welcome(&self, req: WelcomeRequest) -> Result<Response<WelcomeResponse>, Status> {
        let request_id = req.id;
        let d1 = Utc::now() - Duration::days(req.interval as _);
        let d2 = d1 + Duration::days(1);
        let query = QueryRequest::new_with_dt("created_at", d1, d2);
        let mut res_user_stats = self.user_stats.clone().query(query).await?.into_inner();

        let user_stat: Vec<UserStat> =
            sqlx::query_as("SELECT * FROM user_stats where gender = 'female'")
                .fetch_all(&self.sqlx_pool)
                .await
                .map_err(|err| {
                    let code = match err {
                        sqlx::Error::RowNotFound => Code::NotFound,
                        // Add other error mapping as needed
                        _ => Code::Internal,
                    };
                    Status::new(code, err.to_string())
                })?;
        info!("User stat: {:?}", user_stat);
        let emails: Vec<_> = user_stat.iter().map(|stat| stat.email.clone()).collect();
        info!("emails: {:?}", emails);
        let last_visited_at: Vec<chrono::DateTime<chrono::Utc>> =
            user_stat.iter().map(|stat| stat.last_visited_at).collect();
        info!("last_visited_at: {:?}", last_visited_at);
        let genders: Vec<_> = user_stat.iter().map(|stat| stat.gender.clone()).collect();
        info!("Genders: {:?}", genders);

        let contents = self
            .metadata
            .clone()
            .materialize(MaterializeRequest::new_with_ids(&req.content_ids))
            .await?
            .into_inner();

        let contents: Vec<Content> = contents
            .filter_map(|v| async move { v.ok() })
            .collect()
            .await;
        let contents = Arc::new(contents);

        let (tx, rx) = mpsc::channel(1024);

        let sender = self.config.server.sender_email.clone();
        tokio::spawn(async move {
            while let Some(Ok(user)) = res_user_stats.next().await {
                let contents = contents.clone();
                let sender = sender.clone();
                let tx = tx.clone();

                let req = SendRequest::new("Welcome".to_string(), sender, &[user.email], &contents);
                if let Err(e) = tx.send(req).await {
                    warn!("Failed to send message: {:?}", e);
                }
            }
        });
        let reqs = ReceiverStream::new(rx);

        // NOTE: this is an alternative solution
        // let sender = self.config.server.sender_email.clone();
        // let reqs = res.filter_map(move |v| {
        //     let sender: String = sender.clone();
        //     let contents = contents.clone();
        //     async move {
        //         let v = v.ok()?;
        //         Some(gen_send_req("Welcome".to_string(), sender, v, &contents))
        //     }
        // });

        self.notification.clone().send(reqs).await?;

        Ok(Response::new(WelcomeResponse { id: request_id }))
    }
    
        pub async fn remind(&self, req: RemindRequest) -> Result<Response<RemindResponse>, Status> {
            let request_id = req.id.clone();
            let seven_days_ago = Utc::now() - Duration::days(7);
    
            let user_stats: Vec<UserStat> = sqlx::query_as(
                "SELECT * FROM user_stats WHERE last_visited_at < $1 AND array_length(viewed_but_not_started, 1) > 0"
            )
            .bind(seven_days_ago)
            .fetch_all(&self.sqlx_pool)
            .await
            .map_err(|err| {
                let code = match err {
                    sqlx::Error::RowNotFound => Code::NotFound,
                    _ => Code::Internal,
                };
                Status::new(code, err.to_string())
            })?;
    
            let emails: Vec<_> = user_stats.iter().map(|stat| stat.email.clone()).collect();
            let id_vec: Vec<u32> = req.id.split(',')
                .map(|s| s.parse::<u32>().unwrap_or(0))
                .collect();
    
            let contents = self
                .metadata
                .clone()
                .materialize(MaterializeRequest::new_with_ids(id_vec.as_slice()))
                .await?
                .into_inner();
    
            let contents: Vec<Content> = contents
                .filter_map(|v| async move { v.ok() })
                .collect()
                .await;
            let contents = Arc::new(contents);
    
            let (tx, rx) = mpsc::channel(1024);
    
            let sender = self.config.server.sender_email.clone();
            let contents = Arc::new(contents);
            let sender = Arc::new(sender);
            let user_stats = Arc::new(user_stats);

            tokio::spawn(async move {
                let user_stats = Arc::new(user_stats.as_ref().to_vec());
                for user in user_stats.as_ref() {
                    let user = Arc::new(user.clone());
                    let contents = contents.clone();
                    let sender = sender.clone();
                    let mut rng = rand::thread_rng();
    
                    // 从 viewed_but_not_started 列表中随机选取最多 9 个内容进行推荐
                    let mut recommended_content_ids: Vec<_> = user.viewed_but_not_started
                        .iter()
                        .choose_multiple(&mut rng, std::cmp::min(9, user.viewed_but_not_started.len()))
                        .into_iter()
                        .cloned()
                        .collect();
    
                    let req = SendRequest::new("Reminder".to_string(), (*sender).clone(), &[user.email.clone()], &contents);
                    if let Err(e) = tx.send(req).await {
                        warn!("Failed to send message: {:?}", e);
                    }
    
                    // 更新用户的最后电子邮件通知时间
                    sqlx::query("UPDATE user_stats SET last_email_notification = $1 WHERE email = $2")
                        .bind(Utc::now())
                        .bind(&user.email)
                        .execute(&self.sqlx_pool)
                        .await
                        .map_err(|err| {
                            let code = Code::Internal;
                            Status::new(code, err.to_string())
                        });
                }
            });
    
            let resp = RemindResponse {
                id,
            };
            Ok(Response::new(resp))
    }
}