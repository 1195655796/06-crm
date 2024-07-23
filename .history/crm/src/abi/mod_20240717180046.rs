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
        let seven_days_ago = Utc::now() - chrono::Duration::days(7);
    
        // Query for users who haven't visited in the last 7 days
        let query = QueryRequest::new_with_dt("last_visited_at", seven_days_ago, Utc::now());
        let mut res_user_stats = self.user_stats.clone().query(query).await?.into_inner();
    
        // Fetch the user stats from the database
        let user_stats: Vec<user_stat::examples::UserStat> =
            sqlx::query_as("SELECT * FROM user_stats WHERE last_visited_at < $1")
                .bind(seven_days_ago)
                .fetch_all(&self.sqlx_pool)
                .await
                .map_err(|err| {
                    let code = match err {
                        sqlx::Error::RowNotFound => tonic::Code::NotFound,
                        _ => tonic::Code::Internal,
                    };
                    Status::new(code, err.to_string())
                })?;
        info!("User stats: {:?}", user_stats);
    
        // Get email addresses from user stats
        let emails: Vec<_> = user_stats.iter().map(|stat| stat.email.clone()).collect();
        info!("Emails: {:?}", emails);
    
        // Assuming req.id is a comma-separated string like "1,2,3"
        let ids: Vec<u32> = req.id.split(',')
            .map(|id| u32::from_str(id.trim()).expect("Failed to parse ID"))
            .collect();
    
        // Prepare the content metadata
        let contents = self
            .metadata
            .clone()
            .materialize(MaterializeRequest::new_with_ids(&ids))
            .await?
            .into_inner();
    
        let contents: Vec<Content> = contents
            .map(|v| v.expect("Failed to get content"))
            .collect::<Result<Vec<_>, _>>()
            .await?;
        let contents = Arc::new(contents);
    
        let (tx, rx) = mpsc::channel(1024);
    
        let sender = self.config.server.sender_email.clone();
        tokio::spawn(async move {
            while let Some(Ok(user)) = res_user_stats.next().await {
                let contents = contents.clone();
                let sender = sender.clone();
                let tx = tx.clone();
    
                let recommendations = Self::get_recommendations(&user, &contents).await;
                let req = SendRequest::new("Reminder".to_string(), sender, &[user.email], &recommendations);
                if let Err(e) = tx.send(req).await {
                    warn!("Failed to send message: {:?}", e);
                }
            }
        });
        let reqs = ReceiverStream::new(rx);
    
        self.notification.clone().send(reqs).await?;
    
        Ok(Response::new(RemindResponse { id: request_id }))
    }
    
    async fn get_recommendations(user: &user_stat::examples::UserStat, contents: &Arc<Vec<Content>>) -> Vec<Content> {
        let mut recommendations = Vec::new();
        
        if let Some(not_finished) = &user.started_but_not_finished {
            for &content_id in not_finished.iter().take(9) {
                if let Some(content) = contents.iter().find(|&c| c.id == content_id) {
                    recommendations.push(content.clone());
                }
            }
        }
    
        recommendations
    }
}