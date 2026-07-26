use std::{fmt::{self, Display}, path::Path, sync::Arc, time::Duration};

use aws_sdk_s3::{presigning::PresigningConfig, primitives::ByteStream, Client};
use tokio::{fs::File, io::AsyncReadExt, sync::{mpsc::{self, Receiver, Sender}, Semaphore}, time::Instant};

use crate::job_creation_service::service::{JobCreationPayload, Status};


pub struct S3UploadServicePayload {
    pub file_path: String,
    pub total_files: u32,
    pub job_id: String,
    pub retry_count: u32,
    pub file_url: String,
    pub page_number: u32
}

pub struct S3UploadService {
    sender: Sender<S3UploadServicePayload>,
    receiver: Receiver<S3UploadServicePayload>,
    worker: S3UploadWorker
}

#[derive(Clone)]
struct S3UploadWorker {
    s3_client: Client,
    job_creation_tx: Sender<JobCreationPayload>,
}

#[derive(Debug)]
pub enum S3UploadError {
    Failure(String)
}

impl Display for S3UploadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3UploadError::Failure(e) => {
                write!(f, "Service Error: {}", e)
            }
        }
    }
}

impl S3UploadService {

    pub fn new(s3_client: Client, job_creation_tx: Sender<JobCreationPayload>) -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        S3UploadService { sender,  receiver, worker: S3UploadWorker{ s3_client, job_creation_tx } }
    }

    pub fn get_sender(&self) -> Sender<S3UploadServicePayload> {
        self.sender.clone()
    }

    pub async fn run(&mut self) {
        let semaphore = Arc::new(Semaphore::new(10));

        while let Some(job) = self.receiver.recv().await {
            let worker = self.worker.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            tokio::spawn(async move {
                let _permit = permit;
                worker.process(job).await;
            });
        }    
    }
}

impl S3UploadWorker {
    async fn upload_to_s3 (
        &self,
        file_path: &str,
        bucket_name: &str,
        job_id: &str
    ) -> Result<String, S3UploadError> {

        let mut file = File::open(&file_path)
            .await
            .map_err(|e| S3UploadError::Failure(e.to_string()))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .await
            .map_err(|e| S3UploadError::Failure(e.to_string()))?;
        let byte_stream = ByteStream::from(buffer);

        let file_name = Path::new(file_path)
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| S3UploadError::Failure("Invalid filename".into()))?
            .trim()
            .to_string();

        let key = format!("{}/{}", job_id, file_name);

        println!("starting upload");
        let metadata = tokio::fs::metadata(file_path).await.unwrap();
        println!(
            "{} -> {} bytes",
            file_path,
            metadata.len()
        );
        let start = Instant::now();
        self.s3_client
            .put_object()
            .bucket(bucket_name)
            .key(&key)
            .body(byte_stream)
            .send()
            .await
            .map_err(|e| S3UploadError::Failure(e.to_string()))?;
        println!(
            "Upload {} took {:?}",
            file_name,
            start.elapsed()
        );

        let presigned_req = self.s3_client
            .get_object()
            .bucket(bucket_name)
            .key(&key)
            .presigned(
                PresigningConfig::expires_in(Duration::from_secs(3600))
                    .map_err(|e| S3UploadError::Failure(e.to_string()))?,
            )
            .await
            .map_err(|e| S3UploadError::Failure(e.to_string()))?;

        println!("completed upload");
        Ok(presigned_req.uri().to_string())
    }

    async fn process(&self, value: S3UploadServicePayload) {
        //get the file_path, upload it to s3, send the file_path, job_id and status to another service on
        //success and failure. The other service will count success and failure against a job_id.
        //Need to return the s3 path so that the Job creation service can add the s3 path
        println!("uplaoding to s3");
        match self.upload_to_s3(&value.file_path, "fileocr", &value.job_id).await {
            Ok(s3_url) => {
                println!("uplaoded to s3");
                if let Err(e) = self.job_creation_tx.send(
                    JobCreationPayload {
                        job_id: value.job_id.clone(),
                        file_path: value.file_path.clone(),
                        status: Status::Success,
                        total_files: value.total_files,
                        s3_url: Some(s3_url),
                        retry_count: value.retry_count,
                        file_url: value.file_url,
                        page_number: value.page_number
                    }
                ).await.map_err(|e| S3UploadError::Failure(e.to_string())) {
                    eprintln!("Error while sending to Job Creation Service {}", e);
                }
            },
            Err(e) => {
                eprintln!("Error in creating s3 file {}", e);
                if let Err(e) = self.job_creation_tx.send(
                    JobCreationPayload {
                        job_id: value.job_id.clone(),
                        file_path: value.file_path.clone(),
                        status: Status::Failure,
                        total_files: value.total_files,
                        s3_url: None,
                        retry_count: value.retry_count,
                        file_url: value.file_url,
                        page_number: value.page_number
                    }
                ).await.map_err(|e| S3UploadError::Failure(e.to_string())) {
                    eprintln!("Error while sending to Job Creation Service {}", e);
                }
            }
        }
    }
}
