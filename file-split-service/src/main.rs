use file_split_service::split_service::service::run;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error {}", e);
    }
}
