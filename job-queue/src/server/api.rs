use std::{env, error::Error, time::Duration};
use axum::{Json, Router, extract::{Query, State}, http::StatusCode, routing::{get, post}};
use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::oneshot;

use crate::{queue_service::service::{QueueOperation, QueuePayload, QueueService}, server::value::{AppState, GetQueryParams, JobQueueError, Task, TaskType}};

pub async fn run() -> Result<(), Box<dyn Error>> {
    //Initialise queue service, with three queue for each task type.
    //Pass the queue service as a state
    dotenv().ok();

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db = PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("Failed to connect to DB");

    let mut qs = QueueService::new(db.clone());
    let sender = qs.get_sender();
    tokio::spawn(async move {
        qs.execute().await;
    });

    let state = AppState {
        queue_sender: sender,
        db_conn: db
    };

    let app = Router::new()
        .route("/", get(handle_root))
        .route("/task", get(get_task))
        .route("/push", post(push_task))
        .with_state(state);
    
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    println!("Job Queue running on 127.0.0.1:8080...");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_root() -> &'static str {
    "Hello job queue"
}

async fn get_task(
    State(state): State<AppState>,
    Query(params): Query<GetQueryParams>,
) ->Result<Json<Option<Task>>, JobQueueError> {
    let sleep_for = Duration::from_secs(1);
    for _ in 0..params.timeout {
        let (sender, receiver) = oneshot::channel();
        state.queue_sender.send(
            QueuePayload {
                task: None,
                task_type: params.task_type.clone(),
                operation: QueueOperation::Remove,
                sender_tx: Some(sender)
            }
        ).await.map_err(|e| JobQueueError::UnexpectedError(e.to_string()))?;

        let res = receiver.await.map_err(|e| JobQueueError::UnexpectedError(e.to_string()))?;

        match res {
            Ok(Some(task)) => {
                return Ok(Json(Some(task)));
            },
            Ok(None) => {
                //no task
            }
            Err(e) => {
                return Err(JobQueueError::UnexpectedError(e.to_string()));
            }
        }
        tokio::time::sleep(sleep_for).await;
    }
    Ok(Json(None))
}


async fn push_task(
    State(state): State<AppState>,
    Json(payload): Json<Task>,
) -> Result<StatusCode, JobQueueError> {

    let task_type = match &payload {
        Task::Ocr { .. } => TaskType::Ocr,
        Task::Split { .. } => TaskType::Split,
        Task::Aggregate { .. } => TaskType::Aggregate
    };

    state.queue_sender.send(
        QueuePayload { task: Some(payload), task_type, operation: QueueOperation::Insert, sender_tx: None }
    ).await.map_err(|e| JobQueueError::UnexpectedError(e.to_string()))?;
    //TODO: Instead of waiting for response. Immediately send success. Then handle the failure
    //internally through retry queue.
    Ok(StatusCode::ACCEPTED)
}
