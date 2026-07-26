use std::time::Duration;

use reqwest::Client;
use sqlx::{prelude::FromRow, Pool, Postgres};
use tokio::time::sleep;
use uuid::Uuid;

use crate::{job_creation_service::db_utils::{ocr_job_enqueue_fail, JobCreationError}, retry_worker::db_utils::{get_jobs_by_status, update_status_of_jobs}, split_service::value::Task};

pub struct RetryWorker {
    db: Pool<Postgres>,
}

#[derive(Debug, FromRow)]
pub struct RetryWorkerRowResult {
    pub id: Uuid,
    pub file_url: String,
}

impl RetryWorker {
    pub fn new(db: Pool<Postgres>) -> Self {
        Self { db }
    }

    pub async fn execute(&self) {
        loop {
            if let Err(e) = self.retry_enqueue_failed_jobs().await {
                eprintln!("Could not execute retry worker {}", e);
            }
            sleep(Duration::from_secs(10)).await;
        }
    }

    async fn retry_enqueue_failed_jobs(&self) -> Result<(), JobCreationError>{
        let data = get_jobs_by_status(&self.db, "ocr_enqueue_failed".to_string()).await?;
        let mut success_enqueue: Vec<Uuid> = vec![];

        if data.len() == 0 {
            return Ok(())
        }    

        let client  = Client::new();
        let url = "http://job-queue:8080/push";
        
        for f in data {
            let task = Task {
                task_type: "ocr".to_string(),
                job_id: f.id.to_string(),
                file_url: f.file_url.clone(),
                retry_left: 5,
                page_number: Some(f.page_number),
                root_job_id: Some(f.job_id.to_string())
            };
            match client.post(url).json(&task).send().await {
                Ok(_) => success_enqueue.push(f.id),
                Err(_) => {
                    let _ = ocr_job_enqueue_fail(&self.db, &task.job_id).await.map_err(|e| JobCreationError::DBError(e.to_string()));
                }
            }
        }

        update_status_of_jobs(&self.db, success_enqueue, "ocr_enqueue_pending".to_string()).await?;
        Ok(())
    }

}
