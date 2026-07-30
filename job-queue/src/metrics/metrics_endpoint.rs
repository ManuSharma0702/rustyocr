use axum::{
    http::StatusCode, response::{IntoResponse, Response}, routing::get, Router
};
use prometheus::{Encoder, TextEncoder};

use crate::metrics::metrics::REGISTRY;

/// Handler for /metrics endpoint
/// Returns metrics in Prometheus text format
async fn metrics_handler() -> Response {
    // Collect all metrics from the registry
    let metric_families = REGISTRY.gather();

    // Encode metrics in Prometheus text format
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();

    match encoder.encode(&metric_families, &mut buffer) {
        Ok(_) => {
            // Return metrics with correct content type
            (
                StatusCode::OK,
                [(
                    "content-type",
                    "text/plain; version=0.0.4; charset=utf-8",
                )],
                buffer,
            )
                .into_response()
        }
        Err(_) => {
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub fn metrics_router() -> Router {
    Router::new().route("/metrics", get(metrics_handler))
}

