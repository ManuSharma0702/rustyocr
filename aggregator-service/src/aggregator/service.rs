use std::{env, error::Error, fs::File, io::Read, path::{Path, PathBuf}};
use std::io::Write;

use aws_config::load_from_env;
use aws_sdk_s3::{primitives::ByteStream, Client};
use axum::{body::Bytes, extract::multipart::Field};
use dotenvy::dotenv;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

use crate::aggregator::{db_utils::{aggregate_job_enqueue_fail, completed_job_in_db, fetch_results, RowResult}, s3::{upload_to_s3, FileObject}, value::{AggregateServiceError, Task}};



pub async fn run() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db = PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("Failed to connect to DB");

    let config = load_from_env().await;
    let client = Client::new(&config);

    loop {
        match get_aggregate_task().await {
            Ok(Some(val)) => {
                if let Err(e) = process(val.clone(), &db.clone(), client.clone()).await {
                    eprintln!("Error while aggregating {}", e);
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


async fn  process(task: Task, db: &Pool<Postgres>, s3_client: Client) -> Result<(), AggregateServiceError> {
    //TODO: Fetch all results from results table in db using root_job_id as job_id. combine all the
    //results and create a .txt file? and store that file in s3. Store the file key in job table in
    //result_url column and update status to completed, When user tries to fetch the result, if
    //status completed download file from s3 using aws creds. and send back to user
    println!("Aggregating the file");
    let results = fetch_results(db, &task.root_job_id).await?;
    let text_file_path = generate_text_file_from_results(results)?;
    let file_object = generate_file_object(text_file_path, &task)?;
    let file_key = &file_object.file_key.clone();
    match upload_to_s3(&s3_client, file_object, "fileocr").await {
        Ok(_) => {
            completed_job_in_db(db, &task.root_job_id, file_key).await?;
            println!("Agg completed");
        },
        Err(e) => {
            return Err(e);
        }
    }
    Ok(())
}


fn generate_file_object(text_file_path: PathBuf, task: &Task) -> Result<FileObject, AggregateServiceError> {
    let mut file = File::open(&text_file_path)
        .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)
        .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;

    let bytes = Bytes::from(buffer);

    let key = format!("results/{}", task.root_job_id);
     
    let file_object = FileObject {
        file_key: key,
        file_data: bytes
    };

    Ok(file_object)
}

fn generate_text_file_from_results(mut results: Vec<RowResult>) -> Result<PathBuf, AggregateServiceError> {
    if results.len() == 0 {
        return Err(AggregateServiceError::Failed("No data in results".to_string()));
    }
    results.sort_by_key(|r| r.page_number);

    let file_path = std::env::temp_dir().join(
        format!(
            "aggregated_{}.txt",
            results[0].root_job_id.to_string()
        )
    );
    let mut file = File::create(&file_path).map_err(|e| AggregateServiceError::Failed(e.to_string()))?;
    for row in results {
        writeln!(
            file,
            "================= PAGE {} =================",
            row.page_number
        ).map_err(|e| AggregateServiceError::Failed(e.to_string()))?;
        writeln!(file, "{}\n", row.result).map_err(|e| AggregateServiceError::Failed(e.to_string()))?;
    };
    Ok(file_path)
}

async fn get_aggregate_task() -> Result<Option<Task>, AggregateServiceError> {
    let client  = reqwest::Client::new();
    let url = "http://job-queue:8080/task?task_type=aggregate&timeout=10";
    let res = client.get(url)
        .send()
        .await
        .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;
    if res.status().is_success() {
        let task: Option<Task> = res
            .json()
            .await
            .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;
        Ok(task)
    } else {
        Err(AggregateServiceError::Failed("INTERNAL SERVICE ERROR".to_string()))
    }
}

async fn fail_job(db: &Pool<Postgres>, task: Task) {
    let task = Task {
        job_id: task.job_id.clone(),
        task_type: "aggregate".to_string(),
        retry_left: task.retry_left - 1,
        root_job_id: task.root_job_id
    };
    let client  = reqwest::Client::new();
    let url = "http://job-queue:8080/push";
    match client.post(url).json(&task).send().await {
        Ok(_) => (),
        Err(_) => {
            let _ = aggregate_job_enqueue_fail(db, &task.job_id).await.map_err(|e| AggregateServiceError::Failed(e.to_string()));
        }
    }
}
