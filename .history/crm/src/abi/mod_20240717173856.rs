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
use rand_chacha::ChaCha12Rng;
use rand::SeedableRng;


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
        let request_id = req.id;
        let seven_days_ago = Utc::now() - Duration::days(7);
    
        // 查找上次登录距离当前超过7天的用户
        let query = QueryRequest::new_with_dt("last_visited_at", seven_days_ago, Utc::now());
        let mut res_user_stats = self.user_stats.clone().query(query).await?.into_inner();
    
        let user_stats: Vec<UserStat> = sqlx::query_as("SELECT * FROM user_stats WHERE last_visited_at < \$1")
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
    
        info!("User stats: {:?}", user_stats);
    
        let (tx, rx) = mpsc::channel(1024);
    
        let sender = self.config.server.sender_email.clone();
        tokio::spawn(async move {
            while let Some(Ok(user_stat)) = res_user_stats.next().await {
                let tx = tx.clone();
                let sender = sender.clone();
    
                // 从未观看完成列表中随机选取最多9个内容进行推荐
                let content_ids: Vec<i32> = user_stat
                    .started_but_not_finished
                    .choose_multiple(&mut rand::thread_rng(), std::cmp::min(9, user_stat.started_but_not_finished.len()))
                    .cloned()
                    .collect();
    
                let contents = self
                    .metadata
                    .clone()
                    .materialize(MaterializeRequest::new_with_ids(&content_ids))
                    .await?
                    .into_inner();
    
                let req = SendRequest::new("Remind".to_string(), sender, &[user_stat.email], &contents);
                if let Err(e) = tx.send(req).await {
                    warn!("Failed to send message: {:?}", e);
                }
    
                // 更新最近的应用内通知时间
                sqlx::query("UPDATE user_stats SET last_in_app_notification = \$1 WHERE email = \$2")
                    .bind(Utc::now())
                    .bind(user_stat.email)
                    .execute(&self.sqlx_pool)
                    .await
                    .map_err(|err| {
                        let code = match err {
                            sqlx::Error::RowNotFound => Code::NotFound,
                            _ => Code::Internal,
                        };
                        Status::new(code, err.to_string())
                    })?;
    
                // 检查是否需要发送电子邮件通知
                let one_week_ago = Utc::now() - Duration::weeks(1);
                if user_stat.last_email_notification.is_none() || user_stat.last_email_notification.unwrap() < one_week_ago {
                    let req = SendRequest::new("Remind".to_string(), sender, &[user_stat.email], &contents);
                    if let Err(e) = tx.send(req).await {
                        warn!("Failed to send message: {:?}", e);
                    }
    
                    // 更新最近的电子邮件通知时间
                    sqlx::query("UPDATE user_stats SET last_email_notification = \$1 WHERE email = \$2")
                        .bind(Utc::now())
                        .bind(user_stat.email)
                        .execute(&self.sqlx_pool)
                        .await
                        .map_err(|err| {
                            let code = match err {
                                sqlx::Error::RowNotFound => Code::NotFound,
            sqlx::Error::Database(database_error) => {
                if database_error.code().map(|c| c.code()) == Some("23505") {
                    // Unique constraint violation, e.g. email already exists
                    Code::AlreadyExists
                } else {
                    Code::Internal
                }
            }
            _ => Code::Internal,
        };
        Status::new(code, err.to_string())
    })?;
                }}});
        Ok(Response::new(RemindResponse { id: request_id }))
    }}        
    