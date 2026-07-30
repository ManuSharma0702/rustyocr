// src/metrics.rs
// Define application metrics using prometheus crate

use lazy_static::lazy_static;
use prometheus::{IntCounterVec, Opts, Registry};

// Create a custom registry (optional, can use default)
lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    
    pub static ref QUEUE_TASK_COUNT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "queue_task_count",
            "Qeuue task count by task type"
        ),
        &["task_type"]
    ).expect("metric can be created");
}

/// Initialize and register all metrics with the registry
pub fn init_metrics() {
    REGISTRY
        .register(Box::new(QUEUE_TASK_COUNT.clone()))
        .expect("collector can be registered");
}

// Usage examples
pub fn record_task(task_type: &str) {
    // Increment labeled counter
    QUEUE_TASK_COUNT
        .with_label_values(&[task_type])
        .inc();
}

