use aws_sdk_s3::{primitives::ByteStream, Client};

use axum::body::Bytes;

use crate::aggregator::value::AggregateServiceError;

pub  struct FileObject {
    pub file_key: String,
    pub file_data: Bytes
}

pub async fn upload_to_s3(
    s3_client: &Client,
    file: FileObject,
    bucket_name: &str,
) -> Result<(), AggregateServiceError> {
    let key = file.file_key.clone();

    s3_client
        .put_object()
        .bucket(bucket_name)
        .key(&key)
        .body(ByteStream::from(file.file_data))
        .send()
        .await
        .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;
    Ok(())
}
