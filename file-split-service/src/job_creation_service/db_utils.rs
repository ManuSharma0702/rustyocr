use std::{fmt::Display, str::FromStr};

use sqlx::{prelude::FromRow, Pool, Postgres};
use uuid::Uuid;

#[derive(Debug)]
pub enum JobCreationError {
    Failed(String),
    DBError(String)
}

pub struct RowData {
    pub status: Option<String>,
    pub enqueue_left: Option<i32>,
    pub file_url: Option<String>,
    pub job_id: Option<Uuid>,
    pub page_number: Option<i32>,
}

#[derive(Debug, FromRow)]
pub struct RowDataResult {
    pub id: Uuid,
    pub file_url: String,
    pub page_number: i32,
    pub root_job_id: Uuid
}

impl Display for JobCreationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobCreationError::DBError(e) => {
                write!(f, "Database error: {}", e)
            },
            JobCreationError::Failed(e) => {
                write!(f, "Job creation failed {}", e)
            }
        }
    }
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

pub async fn insert_row(
    db_conn: &Pool<Postgres>,
    rows: Vec<RowData>
) -> Result<Vec<RowDataResult>, JobCreationError> {

    if rows.is_empty() {
        return Ok(vec![]);
    }

    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO ocr_jobs (status, file_url, job_id, enqueue_left, page_number) "
    );

    query_builder.push_values(rows.iter(), |mut b, row| {
        b.push_bind(
            row.status.clone().unwrap_or("ocr_enqueue_pending".to_string()),
        )
        .push_bind(
            row.file_url.clone().unwrap_or("".to_string())
        )
        .push_bind(row.job_id.clone())
        .push_bind(row.enqueue_left.unwrap_or(5))
        .push_bind(row.page_number);
    });
    query_builder.push(" RETURNING id, file_url, page_number, job_id as root_job_id");

    let query = query_builder.build_query_as::<RowDataResult>();

    let inserted_rows = query
        .fetch_all(db_conn)
        .await
        .map_err(|e| JobCreationError::DBError(e.to_string()))?;

    Ok(inserted_rows)
}

pub async fn ocr_job_enqueue_fail(db_conn: &Pool<Postgres>, job_id: &str) -> Result<(), JobCreationError> {
    let uuid = Uuid::from_str(job_id)
        .map_err(|e| JobCreationError::DBError(e.to_string()))?;
    
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
    .map_err(|e| JobCreationError::DBError(e.to_string()))?;

    Ok(())
}

pub async fn populate_total_pages_in_jobs_table(db_conn: &Pool<Postgres>, job_id: &str, total_pages: i32) -> Result<(), JobCreationError> {
    let uuid = Uuid::from_str(job_id)
        .map_err(|e| JobCreationError::DBError(e.to_string()))?;
    
    sqlx::query(
        r#"
        UPDATE jobs
        SET
            total_pages = $1
        WHERE id = $2
        "#
    )
    .bind(total_pages)
    .bind(uuid)
    .execute(db_conn)
    .await
    .map_err(|e| JobCreationError::DBError(e.to_string()))?;

    Ok(())
}


