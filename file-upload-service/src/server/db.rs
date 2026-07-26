use sqlx::{Pool, Postgres};
use uuid::Uuid;
use std::str::FromStr;

use crate::{retry_worker_service::service::RetryWorkerRowResult, server::{value::{JobCreationError, RowData, RowDataResult}}};

pub async fn create_job(db_conn: &Pool<Postgres>) -> Result<String, JobCreationError>{
    let row_data = RowData {
        status: Some("split_enqueue_pending".to_string()),
        total_pages: Some(0),
        completed_pages: Some(0),
        enqueue_left: Some(5),
        file_url: None
    };
    insert_row(db_conn, row_data).await
}

pub async fn get_job_by_id(
    db_conn: &Pool<Postgres>,
    job_id: &str,
) -> Result<(String, String), JobCreationError> {
    let uuid = Uuid::from_str(job_id)
        .map_err(|e| JobCreationError::DBError(e.to_string()))?;

    let row = sqlx::query_as::<_, RowDataResult>(
        r#"
        SELECT *
        FROM jobs
        WHERE id = $1
        "#,
    )
    .bind(uuid)
    .fetch_one(db_conn)
    .await
    .map_err(|e| JobCreationError::DBError(e.to_string()))?;

    Ok((row.status, row.result_key.unwrap_or("null".to_string())))
}

pub async fn update_status_of_job(db_conn: &Pool<Postgres>, job_id: &str, status: String) -> Result<(), JobCreationError>{
    let row_data = RowData {
        status: Some(status),
        total_pages: None,
        completed_pages: None,
        enqueue_left: Some(0),
        file_url: None
    };
    update_row(db_conn, row_data, job_id).await
}

pub async fn add_file_url_in_db(db_conn: &Pool<Postgres>, job_id:&str, file_url: String) -> Result<(), JobCreationError> {
    let row_data = RowData { 
        file_url: Some(file_url),
        total_pages: None,
        completed_pages: None,
        enqueue_left: None,
        status: None
    };
    update_row(db_conn, row_data, job_id).await
}

pub async fn job_enqueue_fail(db_conn: &Pool<Postgres>, job_id: &str) -> Result<(), JobCreationError> {
    let uuid = Uuid::from_str(job_id)
        .map_err(|e| JobCreationError::DBError(e.to_string()))?;
    
    sqlx::query(
        r#"
        UPDATE jobs
        SET
            enqueue_left = GREATEST(enqueue_left - 1, 0),
            status = CASE
                WHEN enqueue_left - 1 <= 0 THEN 'dead'
                ELSE 'split_enqueue_failed'
            END
        WHERE id = $1
        "#
    )
    .bind(uuid)
    .execute(db_conn)
    .await
    .map_err(|e| JobCreationError::DBError(e.to_string()))?;

    Ok(())
}

pub async fn get_jobs_by_status(db_conn: &Pool<Postgres>, status: String) -> Result<Vec<RetryWorkerRowResult>, JobCreationError>  {
    sqlx::query_as::<_, RetryWorkerRowResult>(
        r#"
        SELECT * from jobs
        WHERE status = $1
        "#
    )
    .bind(&status)
    .fetch_all(db_conn)
    .await
    .map_err(|e| JobCreationError::DBError(e.to_string()))
}

pub async fn update_status_of_jobs(db_conn: &Pool<Postgres>, job_ids: Vec<Uuid>, status: String) -> Result<(), JobCreationError> {
    let row_data = RowData {
        file_url: None,
        total_pages: None,
        completed_pages: None,
        enqueue_left: None,
        status: Some(status)
    };

    update_multiple_row(db_conn, row_data, job_ids).await?;
    Ok(())
}

async fn update_multiple_row(
    db_conn: &Pool<Postgres>,
    row_data: RowData,
    job_ids: Vec<Uuid>
) -> Result<(), JobCreationError> {
    sqlx::query(
        r#"
        UPDATE jobs
        SET
            status = COALESCE($1, status),
            total_pages = COALESCE($2, total_pages),
            page_completed = COALESCE($3, page_completed),
            enqueue_left = COALESCE($4, enqueue_left),
            file_url = COALESCE($5, file_url)
        WHERE id = ANY($6)
        "#
    )
    .bind(row_data.status)
    .bind(row_data.total_pages)
    .bind(row_data.completed_pages)
    .bind(row_data.enqueue_left)
    .bind(row_data.file_url)
    .bind(&job_ids) 
    .execute(db_conn)
    .await
    .map_err(|e| JobCreationError::DBError(e.to_string()))?;

    Ok(())
}

async fn update_row(
    db_conn: &Pool<Postgres>,
    row_data: RowData,
    job_id: &str
) -> Result<(), JobCreationError> {

    let uuid = Uuid::from_str(job_id)
        .map_err(|e| JobCreationError::DBError(e.to_string()))?;

    sqlx::query(
        r#"
        UPDATE jobs
        SET
            status = COALESCE($1, status),
            total_pages = COALESCE($2, total_pages),
            page_completed = COALESCE($3, page_completed),
            enqueue_left = COALESCE($4, enqueue_left),
            file_url = COALESCE($5, file_url)
        WHERE id = $6
        "#
    )
    .bind(row_data.status)
    .bind(row_data.total_pages)
    .bind(row_data.completed_pages)
    .bind(row_data.enqueue_left)
    .bind(row_data.file_url)
    .bind(uuid)
    .execute(db_conn)
    .await
    .map_err(|e| JobCreationError::DBError(e.to_string()))?;

    Ok(())
}

async fn insert_row(
    db_conn: &Pool<Postgres>,
    row_data: RowData
) -> Result<String, JobCreationError> {

    let data = sqlx::query_as::<_, RowDataResult>(
        r#"
        INSERT INTO jobs (status, page_completed, total_pages, enqueue_left)
        VALUES ($1, $2, $3, $4)
        RETURNING id, status, result_key
        "#
    )
    .bind(row_data.status.unwrap_or("PENDING".to_string()))
    .bind(row_data.completed_pages.unwrap_or(0))
    .bind(row_data.total_pages.unwrap_or(0))
    .bind(row_data.enqueue_left.unwrap_or(5))
    .fetch_one(db_conn)
    .await
    .map_err(|e| JobCreationError::DBError(e.to_string()))?; // fixed below

    Ok(data.id.to_string())
}
