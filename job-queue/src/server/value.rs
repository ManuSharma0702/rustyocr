use axum::{http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};
use tokio::sync::mpsc::Sender;

use crate::queue_service::{service::QueuePayload, value::QueueServiceError};

#[derive(Deserialize, Debug, Clone)]
#[derive(Eq, Hash, PartialEq)]
#[serde(rename_all="lowercase")]
pub enum TaskType {
    Split,
    Ocr,
    Aggregate
}

#[derive(Deserialize, Debug)]
pub struct GetQueryParams {
    pub task_type: TaskType,
    pub timeout: i32
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "task_type", rename_all = "lowercase")]
pub enum Task {
    Split {
        job_id: String,
        file_url: String,
        retry_left: u32,
    },
    Ocr {
        job_id: String,
        file_url: String,
        page_number: u32,
        retry_left: u32,
        root_job_id: String
    },
    Aggregate  {
        job_id: String,
        retry_left: u32,
        root_job_id: String
    }
}

impl Task {
    pub fn task_type(&self) -> TaskType {
        match self {
            Task::Ocr { .. } => TaskType::Ocr,
            Task::Split { .. } => TaskType::Split,
            Task::Aggregate { .. } => TaskType::Aggregate
        }
    }

    pub fn job_id(&self) -> &String {
        match self {
            Task::Ocr { job_id, .. } => job_id,
            Task::Split { job_id, .. } => job_id,
            Task::Aggregate { job_id, .. } => job_id
        }

    }

    pub fn get_retry(&self) -> u32 {
        match self {
            Task::Ocr { retry_left, .. } => *retry_left,
            Task::Split { retry_left, .. } => *retry_left,
            Task::Aggregate { retry_left, .. } => *retry_left
        }
    }

    pub fn validate(&self) -> Result<(), QueueServiceError> {
        match self {
            Task::Split { job_id, file_url, .. } => {
                if job_id.is_empty() {
                    return Err(
                        QueueServiceError::InvalidTask(
                            "Missing job_id".into()
                        )
                    )
                }
                if file_url.is_empty() {
                    return Err(
                        QueueServiceError::InvalidTask(
                            "Missing file_url".into()
                        )
                    );
                }
                Ok(()) 
            }
            Task::Ocr { job_id, file_url, root_job_id, .. } => {
                if job_id.is_empty() {
                    return Err(
                        QueueServiceError::InvalidTask(
                            "Missing job_id".into()
                        )
                    );
                }
                if file_url.is_empty() {
                    return Err(
                        QueueServiceError::InvalidTask(
                            "Missing file_url".into()
                        )
                    );
                }
                if root_job_id.is_empty() {
                    return Err(
                        QueueServiceError::InvalidTask(
                            "Missing root_job_id".into()
                        )
                    );
                }
                Ok(())
            }
            Task::Aggregate { job_id, root_job_id, .. } => {
                if job_id.is_empty() {
                    return Err(
                        QueueServiceError::InvalidTask(
                            "Missing job_id".into()
                        )
                    );
                }
                if root_job_id.is_empty() {
                    return Err(
                        QueueServiceError::InvalidTask(
                            "Missing root_job_id".into()
                        )
                    );
                }
                Ok(())
            }
            
        }
    }
}

pub enum JobQueueError {
    UnexpectedError(String),
    GetTaskCallFailed
}


impl IntoResponse for JobQueueError {
    fn into_response(self) -> axum::response::Response {
        let body = match self {
            JobQueueError::UnexpectedError(e) => "UnexpectedError".to_string() + &e,
            JobQueueError::GetTaskCallFailed => "Could not call get task".to_string()
        };
        (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
    }
}

#[derive(Clone)]
pub struct AppState {
    pub queue_sender: Sender<QueuePayload>,
    pub db_conn: Pool<Postgres>
}
