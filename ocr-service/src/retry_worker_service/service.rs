use std::time::Duration;

use reqwest::Client;
use sqlx::{prelude::FromRow, Pool, Postgres};
use tokio::time::sleep;
use uuid::Uuid;

use crate::{ocr_service::{db_utils::agg_job_enqueue_fail, value::{OcrServiceError, Task}}, retry_worker_service::db_utils::{get_jobs_by_status, update_status_of_jobs}};

pub struct RetryWorker {
    db: Pool<Postgres>,
}

#[derive(Debug, FromRow)]
pub struct RetryWorkerRowResult {
    pub id: Uuid,
    pub job_id: Uuid,
}

impl RetryWorker {
    pub fn new(db: Pool<Postgres>) -> Self {
        Self { db }
    }

    pub async fn execute(&self) {
        loop {
            if let Err(_) = self.retry_enqueue_failed_jobs().await {
                eprintln!("Could not execute retry worker");
            }
            sleep(Duration::from_secs(10)).await;
        }
    }

    async fn retry_enqueue_failed_jobs(&self) -> Result<(), OcrServiceError>{
        let data = get_jobs_by_status(&self.db, "aggregate_enqueue_failed".to_string()).await?;
        if data.len() == 0 {
            return Ok(());
        }

        let mut success_enqueue: Vec<Uuid> = vec![];

        let client  = Client::new();
        let url = "http://job-queue:8080/push";

        for f in data {
            let task = Task {
                task_type: "aggregate".to_string(),
                job_id: f.id.to_string(),
                root_job_id: f.job_id.to_string(),
                file_url: None,
                page_number: None,
                retry_left: 5
            };

            match client.post(url).json(&task).send().await {
                Ok(_) => success_enqueue.push(f.id),
                Err(_) => {
                    let _ = agg_job_enqueue_fail(&self.db, &task.job_id).await.map_err(|e| OcrServiceError::Failed(e.to_string()));
                }
            }
        }
        update_status_of_jobs(&self.db, success_enqueue, "aggregate_enqueue_pending".to_string()).await?;
        Ok(())
    }

}
