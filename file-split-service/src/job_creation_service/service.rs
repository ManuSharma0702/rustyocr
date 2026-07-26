use std::{collections::HashMap, str::FromStr, usize, vec};

use reqwest::Client;
use sqlx::{Pool, Postgres};
use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;

use crate::{job_creation_service::db_utils::{insert_row, job_enqueue_fail, ocr_job_enqueue_fail, populate_total_pages_in_jobs_table, JobCreationError, RowData}, retry_worker::db_utils::update_status_of_jobs, split_service::value::Task};

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum Status {
    Success,
    Failure
}

#[derive(Debug)]
pub struct JobCreationPayload {
    pub job_id: String,
    pub file_path: String,
    pub status: Status,
    pub total_files: u32,
    pub s3_url: Option<String>,
    pub retry_count: u32,
    pub file_url: String,
    pub page_number: u32
}

struct StatusValue {
    total_files: u32,
    retry_count: u32,
    by_status: HashMap<Status, Vec<JobCreationPayload>>,
}

pub struct JobCreationService {
    sender: Sender<JobCreationPayload>,
    receiver: Receiver<JobCreationPayload>,
    status_map: HashMap<String, StatusValue>,
    db: Pool<Postgres>
}

impl JobCreationService {
    pub fn new(db_conn: Pool<Postgres>) -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        let status_map = HashMap::new();
        Self { sender, receiver, status_map, db: db_conn }
    }

    pub async fn run(&mut self) {
        while let Some(val) = self.receiver.recv().await {
            self.count(val).await;
        }
    }

    pub fn get_sender(&self) -> Sender<JobCreationPayload> {
        self.sender.clone()
    }

    async fn process_completion(&self, mut job_status: StatusValue) {
        //If any failure, fail the entire job
        if job_status.by_status.get(&Status::Failure).is_some_and(|x| x.len() > 0) {
            //send back to job queue.
            let v = job_status.by_status.entry(Status::Failure)
                .or_default();
            let task = Task {
                job_id: v[0].job_id.clone(),
                task_type: "split".to_string(),
                file_url: v[0].file_url.clone(),
                retry_left: v[0].retry_count - 1,
                page_number: None,
                root_job_id: None
            };
            let client  = Client::new();
            let url = "http://job-queue:8080/push";
            match client.post(url).json(&task).send().await {
                Ok(_) => (),
                Err(_) => {
                    let _ = job_enqueue_fail(&self.db, &task.job_id).await.map_err(|e| JobCreationError::DBError(e.to_string()));
                }
            }
            return;
        }

        //Populate total pages of file in db in jobs table.
        let v = job_status.by_status.entry(Status::Success).or_default();

        let job_id = &v[0].job_id.clone();
        let total_files = v[0].total_files.clone();

        match populate_total_pages_in_jobs_table(&self.db, &job_id.clone(), total_files as i32).await {
            Ok(_) => (),
            Err(e) =>  {
                eprintln!("Error while inserting into DB: {}",e);
                let _ = job_enqueue_fail(&self.db, job_id).await.map_err(|e| JobCreationError::DBError(e.to_string()));
                return;
            }
        }

        //If all success, then create entry in DB for all success as status ocr_enqueue_pending. then create task for each and push to job queue, if any failure then update status to ocr_enqueue_failed and make a retry worker retry these.
        let mut rows: Vec<RowData> = vec![];
        for j in v {
            let uuid = Uuid::from_str(&j.job_id)
                .map_err(|e| JobCreationError::DBError(e.to_string())).unwrap();
            rows.push(
                RowData { status: Some("ocr_enqueue_pending".to_string()), enqueue_left: Some(5), file_url: j.s3_url.take(), job_id: Some(uuid), page_number: Some(j.page_number as i32) }
            );
        }

        let row_result = match insert_row(&self.db, rows).await {
            Ok(val) => val,
            Err(e) =>  {
                eprintln!("Error while inserting into DB: {}",e);
                let _ = job_enqueue_fail(&self.db, job_id).await.map_err(|e| JobCreationError::DBError(e.to_string()));
                return;
            }
        };

        let mut tasks: Vec<Task> = vec![];
        row_result.into_iter().for_each(
            |r| tasks.push(
                Task { task_type: "ocr".to_string(), job_id: r.id.to_string(), file_url: r.file_url, retry_left: 5, page_number: Some(r.page_number), root_job_id: Some(r.root_job_id.to_string()) }
            )
        );

        let client  = Client::new();
        let url = "http://job-queue:8080/push";

        let mut success = vec![];
        for p in tasks {
            match client.post(url).json(&p).send().await {
                Ok(_) => {
                    dbg!("SENT");
                    let uuid = Uuid::from_str(&p.job_id)
                        .map_err(|e| JobCreationError::DBError(e.to_string())).unwrap();
                    success.push(uuid);
                },
                Err(_) => {
                    let _ = ocr_job_enqueue_fail(&self.db, &p.job_id).await.map_err(|e| JobCreationError::DBError(e.to_string()));
                }
            }
        }
        println!("Job queue pushed from split service");
        if let Err(e) = update_status_of_jobs(&self.db, success, "ocr_enqueue_success".to_string()).await {
            eprintln!("error while updating status of jobs {}", e);
        }
    }

    async fn count(&mut self, val: JobCreationPayload) {
        let job_id = val.job_id.clone();
        let entry = self.status_map
            .entry(job_id.clone())
            .or_insert_with(|| StatusValue { total_files: val.total_files, by_status: HashMap::new(), retry_count: val.retry_count });
        let v = entry.by_status.entry(val.status.clone())
            .or_insert_with(|| Vec::new());
        v.push(val);

        let total_files_received: usize = entry.by_status
            .values()
            .map(|v| v.len())
            .sum();

        //All files processed
        if entry.total_files == total_files_received as u32 {
            if let Some(job_status) = self.status_map.remove(&job_id) {
                self.process_completion(job_status).await;
            }
        }
    }
}
