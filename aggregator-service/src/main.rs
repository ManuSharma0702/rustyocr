use aggregator_service::aggregator::service::run;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error in service: {}", e);
    }
}
