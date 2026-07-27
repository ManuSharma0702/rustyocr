use std::fmt::Display;

use aws_sdk_s3::Client;
use axum::{body::Bytes, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use sqlx::{prelude::FromRow, Pool, Postgres};
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::job_upload_service::api::Task;

#[derive(Debug)]
pub enum JobCreationError {
    Failed,
    AlreadyExists,
    DBError(String)
}

impl Display for JobCreationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobCreationError::DBError(e) => {
                write!(f, "Database error: {}", e)
            },
            JobCreationError::Failed => {
                write!(f, "Job creation failed")
            },
            JobCreationError::AlreadyExists => {
                write!(f, "Job already exists")
            }
        }
    }
}

#[derive(Debug)]
pub enum FileUploadError {
    JobCreationError(JobCreationError),
    S3UploadFailed(String),
    JobQueueFailed,
    NoFileUploaded,
    EnqueueFailed,
    ApiFailure(String)
}

pub  struct FileObject {
    pub file_key: String,
    pub file_data: Bytes
}


pub struct RowData {
    pub status: Option<String>,
    pub total_pages: Option<i32>,
    pub completed_pages: Option<i32>,
    pub enqueue_left: Option<i32>,
    pub file_url: Option<String>
}

#[derive(Debug, FromRow)]
pub struct RowDataResult {
    pub id: Uuid,
    pub status: String,
    pub result_key: Option<String>
}

impl From<JobCreationError> for FileUploadError {
    fn from(err: JobCreationError) -> Self {
        FileUploadError::JobCreationError(err)
    }
}

impl IntoResponse for FileUploadError {
    fn into_response(self) -> axum::response::Response {
        let body = match self {
            Self::S3UploadFailed(e) => e,
            Self::JobQueueFailed => "Job could not be queue in Job queue".to_string(),
            Self::JobCreationError(JobCreationError::AlreadyExists) => "Job cannot be created, it already exists".to_string(),
            Self::JobCreationError(JobCreationError::Failed) => "Job creation failed".to_string(),
            Self::JobCreationError(JobCreationError::DBError(e)) => e,
            Self::NoFileUploaded => "File not uploaded".to_string(),
            Self::EnqueueFailed => "Task could not be enqueue".to_string(),
            Self::ApiFailure(e) => e.to_string()

        };
        (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
    }
}

#[derive(Clone)]
pub struct AppState{
    pub db_conn: Pool<Postgres>,
    pub s3_client: Client,
    pub job_sender: Sender<Task>
}

#[derive(Serialize)]
pub struct JobStatus {
    pub status: String,
    pub download_url: Option<String>
}
