use job_queue::server::api::run;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("Server failed: {:?}", err);
    }
}
