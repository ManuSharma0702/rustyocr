use std::time::Duration;

use sqlx::{prelude::FromRow, Pool, Postgres};
use tokio::{sync::mpsc::Sender, time::sleep};
use uuid::Uuid;

use crate::{job_upload_service::api::Task, server::{db::{get_jobs_by_status, update_status_of_jobs}, value::JobCreationError}};

pub struct RetryWorker {
    db: Pool<Postgres>,
    job_sender: Sender<Task>
}

#[derive(Debug, FromRow)]
pub struct RetryWorkerRowResult {
    pub id: Uuid,
    pub file_url: String,
}

impl RetryWorker {
    pub fn new(db: Pool<Postgres>, sender: Sender<Task>) -> Self {
        Self { db, job_sender: sender }
    }

    pub async fn execute(&self) {
        loop {
            if let Err(_) = self.retry_enqueue_failed_jobs().await {
                eprintln!("Could not execute retry worker");
            }
            sleep(Duration::from_secs(10)).await;
        }
    }

    async fn retry_enqueue_failed_jobs(&self) -> Result<(), JobCreationError>{
        let data = get_jobs_by_status(&self.db, "split_enqueue_failed".to_string()).await?;
        let mut success_enqueue: Vec<Uuid> = vec![];
        for f in data {
            let task = Task {
                task_type: "split".to_string(),
                job_id: f.id.to_string(),
                file_url: f.file_url.clone(),
                retry_left: 5
            };

            match self.job_sender.send(task).await {
                Ok(_) => success_enqueue.push(f.id),
                Err(e) => eprintln!("failed to enqueue retry job: {} {}", f.id, e)
            }
        }
        update_status_of_jobs(&self.db, success_enqueue, "split_enqueue_pending".to_string()).await?;
        Ok(())
    }

}
