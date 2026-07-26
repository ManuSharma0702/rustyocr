use std::fmt::Display;

use serde::{Deserialize, Serialize};

pub enum AggregateServiceError {
    Failed(String),
}

impl Display for AggregateServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateServiceError::Failed(e) => {
                write!(f, "Failure {}", e)
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Task {
    pub task_type: String,
    pub job_id:  String,
    pub retry_left: u32,
    pub root_job_id: String
}
