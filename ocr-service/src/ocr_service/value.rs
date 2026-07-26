use std::fmt::Display;

use serde::{Deserialize, Serialize};

pub enum OcrServiceError {
    Failed(String),
}

impl Display for OcrServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OcrServiceError::Failed(e) => {
                write!(f, "Failure {}", e)
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Task {
    pub task_type: String,
    pub job_id:  String,
    pub file_url: Option<String>,
    pub retry_left: u32,
    pub page_number: Option<i32>,
    pub root_job_id: String
}
