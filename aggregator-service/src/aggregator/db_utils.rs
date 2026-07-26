use std::str::FromStr;

use sqlx::{prelude::FromRow, Pool, Postgres};
use uuid::Uuid;

use crate::aggregator::value::AggregateServiceError;


#[derive(FromRow, Debug)]
pub struct RowResult {
    pub root_job_id: Uuid,
    pub result: String,
    pub page_number: i32
}


pub async fn completed_job_in_db(db: &Pool<Postgres>, root_job_id: &str, result_key: &str) -> Result<(), AggregateServiceError> {
    let uuid = Uuid::from_str(root_job_id).map_err(
        |e| AggregateServiceError::Failed(e.to_string())
    )?;
    sqlx::query(
        r#"
        UPDATE jobs
        SET 
            status = 'completed',
            result_key = $1
        WHERE id = $2
        "#
    )
    .bind(result_key)
    .bind(uuid)
    .execute(db)
    .await
    .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;

    Ok(())
}


pub async fn aggregate_job_enqueue_fail(db_conn: &Pool<Postgres>, job_id: &str) -> Result<(), AggregateServiceError> {
    let uuid = Uuid::from_str(job_id)
        .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;
    
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
    .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;

    Ok(())
}

pub async fn fetch_results(db: &Pool<Postgres>, root_job_id: &str) -> Result<Vec<RowResult>, AggregateServiceError> {
    let uuid = Uuid::from_str(root_job_id)
        .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;
 
    let data = sqlx::query_as::<_, RowResult>(
        r#"
        SELECT
            job_id as root_job_id,
            data as result,
            page_number
        FROM results
        WHERE job_id = $1
        ORDER BY page_number
        "#
    )
    .bind(uuid)
    .fetch_all(db)
    .await
    .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;

    Ok(data)

}

