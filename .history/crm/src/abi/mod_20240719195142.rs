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
use tokio::sync::mpsc::Receiver;
use crm_send::pb::EmailMessage;
use crm_send::pb::InAppMessage;
use crm_send::pb::send_request::Msg;
use std::env;



impl CrmService {
    pub async fn welcome(&self, req: WelcomeRequest) -> Result<Response<WelcomeResponse>, Status> {
        let request_id = req.id.clone();
        println!("Wecome request: {:?}", req);
        let d1 = Utc::now() - Duration::days(365);
        let d2 = d1 + Duration::days(1);
        let query = QueryRequest::new_with_dt("created_at", d1, d2);
        //let user_stats = self.user_stats.clone().query(query.clone()).await?;
        
        //println!("User stats: {:?}", user_stats);
        let mut res_user_stats = self.user_stats.clone().query(query.clone()).await?.into_inner();

        let user_stat: Vec<UserStat> =
            sqlx::query_as("SELECT * FROM user_stats WHERE created_at BETWEEN '2023-07-20T03:12:35+00:00' AND '2023-07-21T03:12:35+00:00'")
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
        
            let (tx, rx) = mpsc::channel(100);
        
            // 将 Vec<gender> 发送到通道中
            for gender in genders {
                tx.send(gender).await.unwrap();
            }
            drop(tx); // 关闭发送端
        
            // 将 Receiver<gender> 转换为 ReceiverStream
            let mut genders_stream = ReceiverStream::new(rx);
        
            // 在此处使用 genders_stream 进行异步处理
            while let Some(r) = genders_stream.next().await {
                info!("gender information: {:?}", r);
            }
            info!("gender conclusion: {:?}", genders_stream.next());
        

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
        //println!("Contents: {:?}", contents);

        let (tx, rx) = mpsc::channel(1024);
        info!("tx: {:?}", tx);
        info!("rx: {:?}", rx);

        let sender = self.config.server.sender_email.clone();
        info!("sender is: {:?}", sender);

        //while let Some(Ok(r)) = res_user_stats.next().await {
        //    println!("res_user_stats_information: {:?}", r);
        //}
        let mut user_stat_stream = futures::stream::iter(user_stat);
        //while let Some(m) = user_stat_stream.next().await {
        //    println!("user_stat_information: {:?}", m);
        //}
        
        tokio::spawn(async move {
            while let Some(user) = user_stat_stream.next().await {
                let contents = contents.clone();
                let sender = sender.clone();
                let tx = tx.clone();

                let req = SendRequest::new("Welcome".to_string(), sender, &[user.email], &contents);
                if let Err(e) = tx.send(req.clone()).await {
                    warn!("Failed to send message: {:?}", e);
                }
        
            }
        });
        let mut reqs = ReceiverStream::new(rx);

        while let Some(r) = reqs.next().await {
            println!("reqs_information:{:?}",r);
        }
        info!("request conclusion: {:?}", reqs.next());
        

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
            println!("Remind request: {:?}", req);
    
            let d1 = Utc::now() - Duration::days(20);
            let d2 = d1 + Duration::days(1);
            let query = QueryRequest::new_with_dt("last_visited_at", d1, d2);
    
            let user_stat: Vec<UserStat> =
                sqlx::query_as("SELECT * FROM user_stats WHERE last_visited_at BETWEEN $1 AND $2")
                    .bind(d1)
                    .bind(d2)
                    .fetch_all(&self.sqlx_pool)
                    .await
                    .map_err(|err| {
                        let code = match err {
                            sqlx::Error::RowNotFound => Code::NotFound,
                            _ => Code::Internal,
                        };
                        Status::new(code, err.to_string())
                    })?;
            info!("User stat: {:?}", user_stat);
    
            let (tx, rx) = mpsc::channel(1024);
            let sender = self.config.server.sender_email.clone();
            let now = Utc::now();
    
            let mut user_stat_stream = futures::stream::iter(user_stat);
    
            tokio::spawn(async move {
                while let Some(user) = user_stat_stream.next().await {
                    let contents = user.started_but_not_finished.choose_multiple(&mut rand::thread_rng(), 9).cloned().collect::<Vec<_>>();
                    let sender = sender.clone();
                    let tx = tx.clone();
    
                    let mut in_app_sent = false;
                    let mut email_sent = false;
    
                    // Check if in-app notification was sent today
                    if  user.last_in_app_notification < now {
                        let req = SendRequest {
                            msg: Some(Msg::InApp(InAppMessage {
                                message_id : uuid::Uuid::new_v4().to_string(),
                                device_id : uuid::Uuid::new_v4().to_string(),
                                title: "Weekly Reminder".to_string(),
                                body: format!("Here are some interesting contents: {:?}",contents),
                            })),
                        };
                        if let Err(e) = tx.send(req).await {
                            warn!("Failed to send in-app message: {:?}", e);
                        } else {
                            in_app_sent = true;
                        }
                    }
    
                    // Check if email notification was sent in the past week
                    if  user.last_email_notification < now - Duration::weeks(1) {
                        let req = SendRequest {
                            msg: Some(Msg::Email(EmailMessage {
                                message_id: uuid::Uuid::new_v4().to_string(),
                                recipients: (&[user.email.clone()]).to_vec(),
                                sender: sender.clone(),
                                subject: "Weekly Reminder".to_string(),
                                body:  format!("Here are some contents you might be interested in: {:?}", contents),
                                

                            })),
                        };
                        if let Err(e) = tx.send(req).await {
                            warn!("Failed to send email message: {:?}", e);
                        } else {
                            email_sent = true;
                        }
                    }
    
                    // Update user notification timestamps
                    if in_app_sent || email_sent {
                        
                        let update_query = sqlx::query!(
                            "UPDATE user_stats SET last_in_app_notification = $1, last_email_notification = $2 WHERE email = $3",
                            if in_app_sent { Some(now) } else { Some(user.last_in_app_notification) },
                            if email_sent { Some(now) } else { Some(user.last_email_notification) },
                            user.email
                        );
                
                    }
                }
            });
    
            let  mut reqs = ReceiverStream::new(rx);
            while let Some(r) = reqs.next().await {
                println!("reqs_information:{:?}",r);
            }
    

            self.notification.clone().send(reqs).await?;
    
            Ok(Response::new(RemindResponse { id: request_id }))
        }
    }