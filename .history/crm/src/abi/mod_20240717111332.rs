pub mod auth;

use crate::{
    pb::{WelcomeRequest, WelcomeResponse},
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

        // Find users who haven't visited in the last 7 days
        let user_stats: Vec<(String, String, Gender, Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>, Vec<i64>, Vec<i64>, Vec<i64>, Vec<i64>, Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>)> = sqlx::query_as(
            "SELECT email, name, gender, created_at, last_visited_at, recent_watched, viewed_but_not_started, started_but_not_finished, finished, last_email_notification, last_in_app_notification, last_sms_notification
             FROM user_stats
             WHERE last_visited_at < \$1"
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

        let (tx, rx) = mpsc::channel(1024);

        let sender = self.config.server.sender_email.clone();
        let metadata = self.metadata.clone();

        tokio::spawn(async move {
            for (email, name, gender, _, _, recent_watched, viewed_but_not_started, started_but_not_finished, finished, _, _, _) in user_stats {
                let sender = sender.clone();
                let metadata = metadata.clone();
                let tx = tx.clone();

                // Select up to 9 unfinished contents
                let incomplete_contents: Vec<Content> = metadata
                    .materialize(MaterializeRequest::new_with_ids(&[
                        &viewed_but_not_started,
                        &started_but_not_finished,
                    ].concat()))
                    .await?
                    .into_inner()
                    .filter_map(|v| async move { v.ok() })
                    .take(9)
                    .collect()
                    .await;

                let req = SendRequest::new("Reminder".to_string(), sender, &[email], &incomplete_contents);
                if let Err(e) = tx.send(req).await {
                    warn!("Failed to send message: {:?}", e);
                }
            }
            Ok::<_, anyhow::Error>(())
        });

        let reqs = ReceiverStream::new(rx);
        self.notification.clone().send(reqs).await?;

        // Send weekly email notification
        let weekly_reqs = user_stats
            .into_iter()
            .filter_map(move |(email, name, gender, _, _, recent_watched, viewed_but_not_started, started_but_not_finished, finished, last_email_notification, _, _)| {
                let sender = sender.clone();
                let metadata = metadata.clone();
                async move {
                    let incomplete_contents: Vec<Content> = metadata
                        .materialize(MaterializeRequest::new_with_ids(&[
                            &viewed_but_not_started,
                            &started_but_not_finished,
                        ].concat()))
                        .await?
                        .into_inner()
                        .filter_map(|v| async move { v.ok() })
                        .take(9)
                        .collect()
                        .await;
                    if last_email_notification.is_none() || last_email_notification.unwrap() < Utc::now() - Duration::weeks(1) {
                        Some(SendRequest::new(
                            "Weekly Reminder".to_string(),
                            sender.clone(),
                            &[email],
                            &incomplete_contents,
                        ))
                    } else {
                        None
                    }
