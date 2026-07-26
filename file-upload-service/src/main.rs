use file_upload_service::server;

#[tokio::main]
async fn main() {
    if let Err(err) = server::api::run().await {
        eprintln!("Server failed:{}", err);
    }
}
