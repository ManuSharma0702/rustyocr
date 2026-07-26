use std::{env, error::Error};

use dotenvy::dotenv;
use reqwest::Client;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use tokio::{fs::{self, File}, io::AsyncWriteExt};

use crate::{ocr_service::{db_utils::{agg_job_enqueue_fail, agg_job_enqueue_success, create_agg_job, ocr_job_enqueue_fail, store_result_in_db, update_page_complete_count}, value::{OcrServiceError, Task}}, retry_worker_service::service::RetryWorker};

pub async fn run() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    let base_dir = "/tmp/files".to_string();
    fs::create_dir_all(&base_dir).await?;

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db = PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("Failed to connect to DB");
    
    let retry_worker = RetryWorker::new(db.clone());

    tokio::spawn(async move {
        retry_worker.execute().await;
    });

    loop {
        match get_ocr_task().await {
            Ok(Some(val)) => {
                if let Err(e) = process(val.clone(), &base_dir, &db.clone()).await {
                    eprintln!("Error while splitting {}", e);
                    fail_job(&db.clone(), val).await;
                }
                continue;
            },
            Ok(None) => {
                continue;
            },
            Err(_) => {
                eprintln!("error");
                continue;
            }
        }
    }
}

async fn get_ocr_task() -> Result<Option<Task>, OcrServiceError> {
    let client  = reqwest::Client::new();
    let url = "http://job-queue:8080/task?task_type=ocr&timeout=10";
    let res = client.get(url)
        .send()
        .await
        .map_err(|e| OcrServiceError::Failed(e.to_string()))?;
    if res.status().is_success() {
        let task: Option<Task> = res
            .json()
            .await
            .map_err(|e| OcrServiceError::Failed(e.to_string()))?;
        Ok(task)
    } else {
        Err(OcrServiceError::Failed("INTERNAL SERVICE ERROR".to_string()))
    }
}

async fn download_file(base_dir: &str, file_url: &str, job_id: &str) -> Result<Option<String>, OcrServiceError> {
    let file_path = format!("{}/{}", base_dir, job_id);
    let mut res = reqwest::get(file_url).await.map_err(|e| OcrServiceError::Failed(e.to_string()))?;
    if !res.status().is_success() {
        return Err(OcrServiceError::Failed(format!(
            "HTTP error: {}",
            res.status()
        )));
    }
    let mut dest = File::create(&file_path).await.map_err(|e| OcrServiceError::Failed(e.to_string()))?;
    loop {
        match res.chunk().await {
            Ok(Some(chunk)) => {
                dest.write_all(&chunk).await.map_err(|e| OcrServiceError::Failed(e.to_string()))?;
            },
            Ok(None) => break,
            Err(e) => {
                return Err(OcrServiceError::Failed(e.to_string()));
            }
        }
    }
    Ok(Some(file_path))
}

async fn process(task: Task, base_dir: &str, db: &Pool<Postgres>) -> Result<(), OcrServiceError> {
    println!("Ocr-ing the file");
    let file_path = match download_file(base_dir, &task.file_url.unwrap(), &task.job_id).await {
        Ok(Some(val)) => val,
        Ok(None) => {
            return Err(OcrServiceError::Failed("FILE NOT FOUND".to_string()));
        },
        Err(e) => {
            return Err(e);
        }
    };
    println!("downlaoded the file");
    let bytes = fs::read(file_path).await.map_err(|e| OcrServiceError::Failed(e.to_string()))?;
    let out = pdf_extract::extract_text_from_mem(&bytes).map_err(|e| OcrServiceError::Failed(e.to_string()))?;
    let page_number = task
        .page_number
        .ok_or(OcrServiceError::Failed("Page number not present".to_string()))?;
    process_completion(db, &task.root_job_id, out, page_number).await?;
    Ok(())
}

async fn process_completion(db: &Pool<Postgres>, root_job_id: &str, result: String, page_number: i32) -> Result<(), OcrServiceError> {
    //TODO:
    //OCR service will push the result to results table, and update the count of page completed in
    //jobs table. When pages completed == total pages we need to create a aggregate task
    //which will need the foreign key of the original job, so we need to find a way to pass the
    //original job id here instead of the ocr_job_id, on any error fail the job.

    store_result_in_db(db, root_job_id, result, page_number).await?;
    //On successful store, update page
    let is_completed = update_page_complete_count(db, root_job_id).await?; 
    if is_completed {
        //if completed, create task in db, on success push task to queue.
        let d = create_agg_job(db, root_job_id).await?;
        let task = Task {
            task_type: "aggregate".to_string(),
            job_id: d.id.to_string(),
            retry_left: 5,
            file_url: None,
            page_number: None,
            root_job_id: root_job_id.to_string()
        };
        push_agg_task(db, task).await?;
    }
    Ok(())
}

async fn push_agg_task(db: &Pool<Postgres>, task: Task) -> Result<(), OcrServiceError> {
    let client  = Client::new();
    let url = "http://job-queue:8080/push";
    match client.post(url).json(&task).send().await {
        Ok(_) => {
            agg_job_enqueue_success(db, &task.job_id).await.map_err(|e| OcrServiceError::Failed(e.to_string()))?;
            println!("Pushed from ocr");
        },
        Err(_) => {
            agg_job_enqueue_fail(db, &task.job_id).await.map_err(|e| OcrServiceError::Failed(e.to_string()))?;
        }
    }
    Ok(())
}

async fn fail_job(db: &Pool<Postgres>, task: Task) {
    let task = Task {
        job_id: task.job_id.clone(),
        task_type: "ocr".to_string(),
        file_url: task.file_url.clone(),
        retry_left: task.retry_left - 1,
        page_number: None,
        root_job_id: task.root_job_id
    };
    let client  = reqwest::Client::new();
    let url = "http://job-queue:8080/push";
    match client.post(url).json(&task).send().await {
        Ok(_) => (),
        Err(_) => {
            let _ = ocr_job_enqueue_fail(db, &task.job_id).await.map_err(|e| OcrServiceError::Failed(e.to_string()));
        }
    }
}
