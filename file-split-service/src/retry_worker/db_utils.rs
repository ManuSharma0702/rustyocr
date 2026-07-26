use sqlx::{prelude::FromRow, Pool, Postgres};
use uuid::Uuid;

use crate::job_creation_service::db_utils::{JobCreationError, RowData};

#[derive(Debug, FromRow)]
pub struct RetryWorkerRowResult {
    pub id: Uuid,
    pub file_url: String,
    pub page_number: i32,
    pub job_id: Uuid
}

pub async fn get_jobs_by_status(db_conn: &Pool<Postgres>, status: String) -> Result<Vec<RetryWorkerRowResult>, JobCreationError>  {
    sqlx::query_as::<_, RetryWorkerRowResult>(
        r#"
        SELECT * from ocr_jobs
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
        page_number: None,
        enqueue_left: None,
        status: Some(status),
        job_id: None
    };

    update_multiple_row(db_conn, row_data, job_ids).await?;
    dbg!("Updated successfully");
    Ok(())
}

async fn update_multiple_row(
    db_conn: &Pool<Postgres>,
    row_data: RowData,
    job_ids: Vec<Uuid>
) -> Result<(), JobCreationError> {
    sqlx::query(
        r#"
        UPDATE ocr_jobs
        SET
            status = COALESCE($1, status)
        WHERE id = ANY($2)
        "#
    )
    .bind(row_data.status)
    .bind(&job_ids) 
    .execute(db_conn)
    .await
    .map_err(|e| JobCreationError::DBError(e.to_string()))?;

    Ok(())
}

