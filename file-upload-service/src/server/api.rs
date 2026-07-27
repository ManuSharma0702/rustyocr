use std::env;
use std::error::Error;
use dotenvy::dotenv;

use axum::extract::{self, Multipart, Path, State};
use axum::http::{StatusCode};
use axum::{Json, Router};
use axum::routing::{get, post};
use reqwest::{header, Method};
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::CorsLayer;

use crate::job_upload_service::api::{Task, UploadService};
use crate::retry_worker_service::service::RetryWorker;
use crate::server::db::{add_file_url_in_db, create_job, get_job_by_id, update_status_of_job};
use crate::server::s3::{get_file_from_s3, upload_to_s3};
use crate::server::value::{AppState, FileObject, FileUploadError, JobStatus};
use aws_config::load_from_env;
use aws_sdk_s3::Client;

//On init, I need a db connection for job creation
//An s3 connection for upload
//A job queue connection. These connection will be shared
pub async fn run() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db = PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("Failed to connect to DB");

    sqlx::migrate!().run(&db).await.expect("Migrations failed");

    let config = load_from_env().await;
    let client = Client::new(&config);
    let mut upload_service = UploadService::new(db.clone());
    let upload_sender = upload_service.get_sender();
    let retry_worker = RetryWorker::new(db.clone(), upload_service.get_sender());

    tokio::spawn(async move {
        upload_service.execute().await;
    });
    tokio::spawn(async move {
        retry_worker.execute().await;
    });

    let state = AppState {
        db_conn: db,
        s3_client: client,
        job_sender: upload_sender
    };
    
    let cors = CorsLayer::new()
        .allow_origin(
            ["http://localhost:5173".parse::<axum::http::HeaderValue>().unwrap(),
            "http://localhost:3000".parse::<axum::http::HeaderValue>().unwrap()])
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::CONTENT_TYPE,
        ]);

    let app = Router::new()
        .route("/health", get(handle_home))
        .route("/upload", post(handle_file_upload))
        .route("/jobs/{job_id}", get(get_job_status))
        .layer(cors)
        .with_state(state);
        
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await?;
    println!("Server running on 127.0.0.1:8000...");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_home() -> StatusCode {
    StatusCode::OK
}

async fn get_job_status(
    State(state): State<AppState>,
    Path(id): Path<String>
) -> Result<Json<JobStatus>, FileUploadError> {
    let data = get_job_by_id(&state.db_conn, &id).await?;
    if data.0.eq("completed") {
        //Get the temporary file url from s3 and return that to the user
        let download_url = get_file_from_s3(&state.s3_client, &data.1, "fileocr").await?;
        return Ok(extract::Json(JobStatus {
            status: data.0,
            download_url: Some(download_url)
        }));
    }
    Ok(extract::Json(JobStatus { status: data.0, download_url: None }))
}

//Endpoint for uploading PDFs only
//Creates a job in DB, initialising the process
//Uploads the PDF to s3
//Push splitting task to job queue
async fn handle_file_upload(
    State(state): State<AppState>,
    multipart: Multipart
) -> Result<(StatusCode, String), FileUploadError> {
    let file = accept_form(multipart)
        .await
        .ok_or(FileUploadError::NoFileUploaded)?;

    let job_id = create_job(&state.db_conn).await?; 
    println!("Job created successfully");

    let file_url = match upload_to_s3(&state.s3_client, file, "fileocr").await {
        Ok(val) => {
            //TODO: upload the file_url in db
            add_file_url_in_db(&state.db_conn, &job_id, val.clone()).await?;
            val
        },
        Err(e) => {
            let _ = update_status_of_job(&state.db_conn, &job_id, "dead".to_string()).await;
            return Err(e);
        }
    };

    //TODO: Add to job queue
    //Use a separate job upload service through a Sender. Just send the payload, fire and
    //forget, to return success immediately instead of waiting here with recv.await and block the
    //response. Now if the upload api returns failure then make the upload service update the status of job in db as enqueue
    //failed. Then create a background worker which looks for split_enqueue_failed jobs and retry them.
    //This retry logic should be implemented for each service separately.

    //If the channel fails, then respond with error to the user, and fail the job in DB.
    match state.job_sender.send(
        Task {
            task_type: "split".to_string(),
            job_id: job_id.clone(),
            file_url,
            retry_left: 5 
        }
    ).await {
        Ok(_) => (),
        Err(_)  => {
            let _ = update_status_of_job(&state.db_conn, &job_id, "dead".to_string()).await;
            return Err(FileUploadError::EnqueueFailed);
        }
    };

    println!("Sent to job_queue from file-upload");
    Ok((StatusCode::OK, job_id))
}

async fn accept_form(mut multipart: Multipart) -> Option<FileObject> {
    if let Some(field) = multipart.next_field().await.unwrap() {
        let file_name = field.file_name()?.to_string();
        let data = field.bytes().await.unwrap();
        return Some(FileObject {
            file_key: file_name,
            file_data: data,
        });
    }
    None
}
