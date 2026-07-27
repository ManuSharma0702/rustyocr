use aws_sdk_s3::{primitives::ByteStream, Client};

use crate::server::value::{FileObject, FileUploadError};

use aws_sdk_s3::presigning::PresigningConfig;
use std::time::Duration;

pub async fn upload_to_s3(
    s3_client: &Client,
    file: FileObject,
    bucket_name: &str,
) -> Result<String, FileUploadError> {
    let key = file.file_key.clone();

    match s3_client
        .put_object()
        .bucket(bucket_name)
        .key(&key)
        .body(ByteStream::from(file.file_data))
        .send()
        .await
    {
        Ok(_) => {}
        Err(e) => {
            eprintln!("AWS ERROR: {:#?}", e);

            let mut source = std::error::Error::source(&e);
            while let Some(err) = source {
                eprintln!("Caused by: {}", err);
                source = err.source();
            }

            return Err(FileUploadError::S3UploadFailed(format!("{:#?}", e)));
        }
    }

    println!("Pushed to s3");
    let presigned_req = s3_client
        .get_object()
        .bucket(bucket_name)
        .key(&key)
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(3600))
                .map_err(|e| FileUploadError::S3UploadFailed(e.to_string()))?,
        )
        .await
        .map_err(|e| FileUploadError::S3UploadFailed(e.to_string()))?;

    println!("uploaded to s3");
    Ok(presigned_req.uri().to_string())
}

pub async fn get_file_from_s3(
    s3_client: &Client,
    file_key: &str,
    bucket_name: &str
) -> Result<String, FileUploadError> {
   let presigned_req = s3_client
        .get_object()
        .bucket(bucket_name)
        .key(file_key)
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(3600))
                .map_err(|e| FileUploadError::S3UploadFailed(e.to_string()))?,
        )
        .await
        .map_err(|e| FileUploadError::S3UploadFailed(e.to_string()))?;
    Ok(presigned_req.uri().to_string())
}
