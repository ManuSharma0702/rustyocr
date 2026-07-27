use std::str::FromStr;

use reqwest::Client;
use serde::Serialize;
use sqlx::{Pool, Postgres};
use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;

use crate::server::db::{job_enqueue_fail, update_status_of_jobs};


//Split Task
#[derive(Debug, Serialize)]
pub struct Task {
    pub task_type: String,
    pub job_id:  String,
    pub file_url: String,
    pub retry_left: u32
}

pub struct UploadServicePayload {
    pub task: Task,
}

#[derive(Debug)]
pub enum UploadError {
    NotFound,
    UploadFailed(String),
    Failed(String)
}

pub struct UploadService {
    upload_service_tx: Sender<Task>,
    upload_service_rx: Receiver<Task>,
    db: Pool<Postgres>,
}

impl UploadService {

    pub async fn execute(&mut self) {
        while let Some(task) = self.upload_service_rx.recv().await {
            if let Err(err) = self.upload(task).await {
                eprintln!("Upload failed: {:?}", err);
            }
        }
    }

    async fn upload(&self, payload: Task) -> Result<(), UploadError> {
        let client  = Client::new();

        let url = "http://job-queue:8080/push";

        //TODO:On error update the status of the job in db to split_enqueue_failed.
        //Keep a background worker which loops and checks for split_enqueue_failed jobs  and retry them.
        //Add a new column to DB called enqueue_attempt_left. On each queue failure decrement the
        //count. If count zero then instead of split_enqueue_failed, update status to dead.
        let mut success = vec![];
        match client.post(url).json(&payload).send().await {
            Ok(_) => {
                let uuid = Uuid::from_str(&payload.job_id)
                    .map_err(|e| UploadError::Failed(e.to_string())).unwrap();
                success.push(uuid);
            },
            Err(e) => {
                eprintln!("Queue request failed: {:#?}", e);
                let _ = job_enqueue_fail(&self.db, &payload.job_id).await.map_err(|e| UploadError::UploadFailed(e.to_string()));
                return Err(UploadError::UploadFailed(e.to_string()));
            }
        }
        // .map_err(|e| UploadError::UploadFailed(e.to_string()))?;

        update_status_of_jobs(&self.db, success, "split_enqueue_success".to_string()).await.map_err(|e| UploadError::Failed(e.to_string()))?;
        Ok(())
    }

    pub fn new(db: Pool<Postgres>) -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        Self { upload_service_tx: sender, upload_service_rx: receiver, db }
    }

    pub fn get_sender(&self) -> Sender<Task> {
        self.upload_service_tx.clone()
    }

}

