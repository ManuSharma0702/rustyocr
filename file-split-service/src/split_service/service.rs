use std::{env, error::Error};
use dotenvy::dotenv;
use aws_config::load_from_env;
use aws_sdk_s3::Client;
use lopdf::Document;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use tokio::{fs::{self, File}, io::AsyncWriteExt, sync::mpsc::Sender};

use crate::{job_creation_service::{db_utils::job_enqueue_fail, service::JobCreationService}, retry_worker::service::RetryWorker, s3_upload_service::service::{S3UploadService, S3UploadServicePayload}, split_service::value::{SplitServiceError, Task}};

pub async fn run() -> Result<(), Box<dyn Error>> {
    //On init create a tmp directory for holding files.
    //Base file downloaded from s3 which will be split into pages and then each page will 
    //be uploaded to s3 in a directory by file_name and for each page a new task will be 
    //pushed to job queue for OCR with task having job_id, file_url (page), page_number, retry_left

    dotenv().ok();

    let base_dir = "/tmp/files".to_string();
    fs::create_dir_all(&base_dir).await?;

    //Create a s3 client and pass to s3 upload service
    let config = load_from_env().await;
    let client = Client::new(&config);

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db = PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("Failed to connect to DB");

    let mut job_creation_service = JobCreationService::new(db.clone());
    let mut s3_service = S3UploadService::new(client, job_creation_service.get_sender());

    let s3_service_tx = s3_service.get_sender();
    let retry_worker = RetryWorker::new(db.clone());

    tokio::spawn(async move {
        retry_worker.execute().await;
    });
    tokio::spawn(async move {
        job_creation_service.run().await;
    });
    tokio::spawn(async move {
        s3_service.run().await;
    });

    loop {
        match get_split_task().await {
            Ok(Some(val)) => {
                if let Err(e) = process(val.clone(), &base_dir, s3_service_tx.clone()).await {
                    fail_job(&db.clone(), val).await;
                    eprintln!("Error while splitting {}", e);
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

async fn get_split_task() -> Result<Option<Task>, SplitServiceError> {

    let client  = reqwest::Client::new();

    let url = "http://job-queue:8080/task?task_type=split&timeout=10";

    let res = client.get(url)
        .send()
        .await
        .map_err(|e| SplitServiceError::FetchFailed(e.to_string()))?;

    if res.status().is_success() {
        let task: Option<Task> = res
            .json()
            .await
            .map_err(|_| SplitServiceError::InvalidResponse)?;
        Ok(task)
    } else {
        Err(SplitServiceError::FetchFailed("INTERNAL SERVICE ERROR".to_string()))
    }
}

async fn download_file(base_dir: &str, file_url: &str, job_id: &str) -> Result<Option<String>, SplitServiceError> {
    let file_path = format!("{}/{}_basefile", base_dir, job_id);
    let mut res = reqwest::get(file_url).await.map_err(|e| SplitServiceError::FetchFailed(e.to_string()))?;
    if !res.status().is_success() {
        return Err(SplitServiceError::Failed);
    }
    let mut dest = File::create(&file_path).await.map_err(|e| SplitServiceError::IOError(e.to_string()))?;
    loop {
        match res.chunk().await {
            Ok(Some(chunk)) => {
                dest.write_all(&chunk).await.map_err(|e| SplitServiceError::IOError(e.to_string()))?;
            },
            Ok(None) => break,
            Err(e) => {
                return Err(SplitServiceError::FetchFailed(e.to_string()));
            }
        }
    }
    Ok(Some(file_path))
}

async fn process(task: Task, base_dir: &str, s3_service_tx: Sender<S3UploadServicePayload>) -> Result<(), SplitServiceError> {
    //After successful processing, empty the files directory, but do not delete it.
    println!("Splitting the file");
    let file_path = match download_file(base_dir, &task.file_url, &task.job_id).await {
        Ok(Some(val)) => val,
        Ok(None) => {
            return Err(SplitServiceError::InvalidResponse)
        },
        Err(e) => {
            return Err(e)
        }
    };
    println!("Downloaded the file");

    //Split the files, save to directory then send to file uploader service which uploads to s3
    let doc = Document::load(&file_path).map_err(|e| {
        eprintln!("{}", e);
        return SplitServiceError::FileNotFound;
    })?;
    let pages = doc.get_pages();

    for (i, _) in pages.iter().enumerate() {
        let page_number = (i + 1) as u32;
        
        // Load the document again for each page extraction
        let mut doc = Document::load(&file_path).map_err(|_| SplitServiceError::FileNotFound)?;
        
        // Retain only the current page (1-indexed)
        let pages_to_delete: Vec<u32> = pages
            .keys()
            .filter(|&&p| p != page_number)
            .cloned()
            .collect();
        doc.delete_pages(&pages_to_delete);
        
        // Save the new document
        let output_name = format!("{}_page_{}.pdf", file_path, page_number);
        doc.save(&output_name).map_err(|_| SplitServiceError::Failed)?;
        s3_service_tx.send(
            S3UploadServicePayload {
                file_path: output_name,
                job_id: task.job_id.clone(),
                total_files: pages.len() as u32,
                retry_count: task.retry_left,
                file_url: task.file_url.clone(),
                page_number
            }
        ).await.map_err(|_| SplitServiceError::Failed)?;
    }

    println!("completed splitting");

    //perform splits, get all splitted files and upload all to s3 at the same time, by sending to a
    //upload service which will upload to s3, the upload service will then send to a service which will count the success
    //and if even one fail then fail the job
    //and send back to queue with a reduced retry count.
    //If all success then create job in db with status ocr_enqueue_pending for each then go ahead and upload to job queue. On failure then update status to ocr_enqueue_failed. A bg worker will retry the failed tasks
    //Retry worker will fetch these records by claiming. by getting the rows through update query
    //so that only a single worker will get and other workers which were also queuing will not
    //get. Make sure to put a limit on the query response to make sure a single worker does not
    //retry everything

    Ok(())
}

async fn fail_job(db: &Pool<Postgres>, task: Task) {
    let task = Task {
        job_id: task.job_id.clone(),
        task_type: "split".to_string(),
        file_url: task.file_url.clone(),
        retry_left: task.retry_left - 1,
        page_number: None,
        root_job_id: None
    };
    let client  = reqwest::Client::new();
    let url = "http://job-queue:8080/push";
    match client.post(url).json(&task).send().await {
        Ok(_) => (),
        Err(_) => {
            let _ = job_enqueue_fail(db, &task.job_id).await.map_err(|e| SplitServiceError::Failed);
        }
    }
}
