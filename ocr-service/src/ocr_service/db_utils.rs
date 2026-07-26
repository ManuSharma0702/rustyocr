use std::str::FromStr;

use sqlx::{prelude::FromRow, Pool, Postgres, Row};
use uuid::Uuid;

use crate::ocr_service::value::OcrServiceError;

struct RowData {
    root_job_id: Uuid,
    result: String,
    page_number:i32
}

#[derive(FromRow)]
pub struct RowDataResult {
    id: Uuid,
    root_job_id: Uuid
}

pub struct AggregateRowData {
    pub job_id: Option<Uuid>,
    pub status: Option<String>,
    pub enqueue_left: Option<i32>
}

#[derive(FromRow)]
pub struct AggregateRowResult {
    pub id: Uuid,
    pub root_job_id: Uuid
}

pub async fn ocr_job_enqueue_fail(db_conn: &Pool<Postgres>, job_id: &str) -> Result<(), OcrServiceError> {
    let uuid = Uuid::from_str(job_id)
        .map_err(|e| OcrServiceError::Failed(e.to_string()))?;
    
    sqlx::query(
        r#"
        UPDATE ocr_jobs
        SET
            enqueue_left = GREATEST(enqueue_left - 1, 0),
            status = CASE
                WHEN enqueue_left - 1 <= 0 THEN 'dead'
                ELSE 'ocr_enqueue_failed'
            END
        WHERE id = $1
        "#
    )
    .bind(uuid)
    .execute(db_conn)
    .await
    .map_err(|e| OcrServiceError::Failed(e.to_string()))?;

    Ok(())
}

async fn insert_row(db: &Pool<Postgres>, row: RowData) -> Result<RowDataResult, OcrServiceError> {
    let data = sqlx::query_as::<_, RowDataResult>(
        r#"
        INSERT INTO results (job_id, data, page_number)
        VALUES ($1, $2, $3)
        RETURNING id, job_id as root_job_id
        "#
    )
    .bind(row.root_job_id)
    .bind(row.result)
    .bind(row.page_number)
    .fetch_one(db)
    .await
    .map_err(|e| OcrServiceError::Failed(e.to_string()))?;
    Ok(data)
}

pub async fn store_result_in_db(db: &Pool<Postgres>, root_job_id: &str, result: String, page_number: i32) -> Result<RowDataResult, OcrServiceError> {
    let uuid = Uuid::from_str(&root_job_id).map_err(
        |e| OcrServiceError::Failed(e.to_string())
    )?;
    let row_data = RowData {
        root_job_id: uuid,
        result,
        page_number
    };
    let res = insert_row(db, row_data).await?;
    Ok(res)
}

//Returns whether the job is completed or not by checking if page_completed == total pages
pub async fn update_page_complete_count(db: &Pool<Postgres>, root_job_id: &str) -> Result<bool, OcrServiceError> {
    let uuid = Uuid::from_str(&root_job_id).map_err(
        |e| OcrServiceError::Failed(e.to_string())
    )?;

    let row = sqlx::query(
        r#"
        UPDATE jobs
        SET 
            page_completed = page_completed + 1
        WHERE id = $1
        RETURNING page_completed, total_pages
        "#
    )
    .bind(uuid)
    .fetch_one(db)
    .await
    .map_err(|e| OcrServiceError::Failed(e.to_string()))?;

    let page_completed: i32 = row.get("page_completed");
    let total_pages: i32 = row.get("total_pages");
    Ok(page_completed == total_pages)
}


pub async fn create_agg_job(db: &Pool<Postgres>, root_job_id: &str) -> Result<AggregateRowResult, OcrServiceError> {
    let uuid = Uuid::from_str(&root_job_id).map_err(
        |e| OcrServiceError::Failed(e.to_string())
    )?;
    let row_data = AggregateRowData {
        job_id: Some(uuid),
        status: Some("aggregate_enqueue_pending".to_string()),
        enqueue_left: Some(5)
    };
    insert_agg_job_in_db(db, row_data).await
}

async fn insert_agg_job_in_db(db: &Pool<Postgres>, row: AggregateRowData) -> Result<AggregateRowResult, OcrServiceError> {
    let data = sqlx::query_as::<_, AggregateRowResult>(
        r#"
            INSERT INTO aggregate_jobs (job_id, status, enqueue_left) 
            VALUES ($1, $2, $3)
            RETURNING id, job_id as root_job_id
        "#
    )
    .bind(row.job_id.clone())
    .bind(row.status.clone().unwrap_or("aggregate_enqueue_pending".to_string()))
    .bind(row.enqueue_left.unwrap_or(5))
    .fetch_one(db)
    .await
    .map_err(|e| OcrServiceError::Failed(e.to_string()))?;

    Ok(data)
}

pub async fn agg_job_enqueue_fail(db: &Pool<Postgres>, job_id: &str) -> Result<(), OcrServiceError> {
    let uuid = Uuid::from_str(job_id)
        .map_err(|e| OcrServiceError::Failed(e.to_string()))?;
    sqlx::query(
        r#"
        UPDATE aggregate_jobs
        SET
            enqueue_left = GREATEST(enqueue_left - 1, 0),
            status = CASE
                WHEN enqueue_left - 1 <= 0 THEN 'dead'
                ELSE 'aggregate_enqueue_failed'
            END
        WHERE id = $1
        "#
    )
    .bind(uuid)
    .execute(db)
    .await
    .map_err(|e| OcrServiceError::Failed(e.to_string()))?;
    Ok(())
}

pub async fn agg_job_enqueue_success(db: &Pool<Postgres>, job_id: &str) -> Result<(), OcrServiceError> {
    let uuid = Uuid::from_str(job_id)
        .map_err(|e| OcrServiceError::Failed(e.to_string()))?;
    sqlx::query(
        r#"
        UPDATE aggregate_jobs
        SET
            enqueue_left = GREATEST(enqueue_left - 1, 0),
            status = CASE
                WHEN enqueue_left - 1 <= 0 THEN 'dead'
                ELSE 'aggregate_enqueue_success'
            END
        WHERE id = $1
        "#
    )
    .bind(uuid)
    .execute(db)
    .await
    .map_err(|e| OcrServiceError::Failed(e.to_string()))?;
    Ok(())
}
