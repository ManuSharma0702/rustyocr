use std::fmt::{write, Display, Formatter, Result};

#[derive(Debug)]
pub enum QueueServiceError {
    UnableToInsert,
    QueueNotFound,
    NoTaskFoundToInsert,
    DbFailure(String),
    InvalidUuid,
    InvalidTask(String)
}

impl Display for QueueServiceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            QueueServiceError::NoTaskFoundToInsert => {
                write!(f, "No Task Found to Insert")
            }
            QueueServiceError::QueueNotFound => {
                write!(f, "Queue not found")
            }
            QueueServiceError::UnableToInsert => {
                write!(f, "Not Able to insert")
            }
            QueueServiceError::DbFailure(e) => {
                write!(f, "DB error {}", e)
            }
            QueueServiceError::InvalidUuid => {
                write!(f, "Invalid uuid")
            }
            QueueServiceError::InvalidTask(e) => {
                write!(f, "Error in Task validation: {}", e)
            }
        }
    }
}
