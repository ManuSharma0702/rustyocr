use std::{collections::{HashMap, VecDeque}, str::FromStr};

use sqlx::{Pool, Postgres};
use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;

use crate::{queue_service::value::QueueServiceError, server::value::{Task, TaskType}};

pub struct TaskQueues {
    pending: VecDeque<Task>,
    in_progress: VecDeque<Task>,
    failed: VecDeque<Task>
}

impl TaskQueues {
    fn new() -> Self {
        Self { 
            pending: VecDeque::new(), 
            in_progress: VecDeque::new(), 
            failed: VecDeque::new()
        }
    }
}

pub enum QueueOperation {
    Insert,
    Remove
}

pub type QueueResultPayload = Result<Option<Task>, QueueServiceError>;

//If Insert operation then we need to send Task, if remove operation then we do not need to send
//Task, Task will be received
pub struct QueuePayload {
    pub task: Option<Task>,
    pub task_type: TaskType,
    pub operation: QueueOperation,
    pub sender_tx: Option<tokio::sync::oneshot::Sender<QueueResultPayload>>
}

pub struct QueueService {
    queues: HashMap<TaskType, TaskQueues>,
    queue_service_tx: Sender<QueuePayload>,
    queue_service_rx: Receiver<QueuePayload>,
    db_conn: Pool<Postgres>
}

impl QueueService {
    pub fn new(db: Pool<Postgres>) -> Self {
        let mut hashmap = HashMap::new();
        let (sender, receiver) = mpsc::channel(1024);
        hashmap.insert(TaskType::Ocr, TaskQueues::new());
        hashmap.insert(TaskType::Split, TaskQueues::new());
        hashmap.insert(TaskType::Aggregate, TaskQueues::new());
        Self { queues: hashmap, queue_service_tx: sender, queue_service_rx: receiver, db_conn: db }
    }

    pub async fn execute(&mut self) {
        while let Some(payload) = self.queue_service_rx.recv().await {
            let res = match payload.operation {
                QueueOperation::Insert => {
                    match payload.task {
                        Some(task) => {
                            match self.insert(task).await {
                                Ok(_) => Ok(None),
                                Err(e) => {
                                    eprintln!("Error while inserting into queue {}", e);
                                    Err(e)
                                }
                            }
                        }
                        None => Err(QueueServiceError::NoTaskFoundToInsert),
                    }
                }
                QueueOperation::Remove => self.remove(payload.task_type),
            };

            if let Some(sender) = payload.sender_tx {
                let _ = sender.send(res);
            }
        }
    }

    pub fn get_sender(&self) -> Sender<QueuePayload> {
        self.queue_service_tx.clone()
    }

    async fn insert(&mut self, task: Task) -> Result<(), QueueServiceError> {
        task.validate()?;
        let task_type = task.task_type();
        let retry_left = task.get_retry();

        if retry_left == 0 {
            self.fail_job(&task.job_id()).await?;

            {
                let queue = self.queues.get_mut(&task_type)
                    .ok_or(QueueServiceError::QueueNotFound)?;
                queue.failed.push_back(task);
            }

            return Ok(());
        }

        {
            let queue = self.queues.get_mut(&task_type)
                .ok_or(QueueServiceError::QueueNotFound)?;
            queue.pending.push_back(task);
        }

        Ok(())
    }

    fn remove(&mut self, task_type: TaskType) -> Result<Option<Task>, QueueServiceError> {
        let queue = self
            .queues
            .get_mut(&task_type)
            .ok_or(QueueServiceError::QueueNotFound)?;

        let task = queue.pending.pop_front();
        //Cannot take ownership of Task fields and pass it to inprogress queue, since we need to return the
        //Task back to the caller
        if let Some(ref t) = task {
            queue.in_progress.push_back(t.clone());
        }

        Ok(task)
    }

    async fn fail_job(&mut self, job_id: &str) -> Result<(), QueueServiceError> {
        let d = Uuid::from_str(&job_id)
            .map_err(|e| QueueServiceError::InvalidUuid)?;
    
        sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'dead'
            where id = $1
            "#
        )
        .bind(d)
        .execute(&self.db_conn)
        .await
        .map_err(|e| QueueServiceError::DbFailure(e.to_string()))?;
        Ok(())
    }

}

