# RustyOCR

RustyOCR is a distributed OCR pipeline built using Rust microservices. It processes uploaded PDF documents asynchronously through a queue-based architecture and includes a React web interface for uploading files and downloading the processed output.

## Architecture

```text
Client
   │
   ▼
React UI
   │
   ▼
File Upload Service
   │
   ▼
Job Queue
   │
   ▼
File Split Service
   │
   ▼
OCR Workers
   │
   ▼
Aggregator Service
   │
   ▼
Storage (S3/Local)
```

## Repository Structure

```text
rustyocr/
├── ui/                     # React + TypeScript frontend
├── file-upload-service/
├── job-queue/
├── file-split-service/
├── ocr-service/
├── aggregator-service/
├── docker-compose.yml
├── k8s/                    # Kubernetes manifests
└── README.md
```

## Prerequisites

* Rust (latest stable)
* Docker & Docker Compose
* Git

## Running the Application

Build and start all services, including the React UI:

```bash
docker compose up --build
```

Or, if the images have already been built:

```bash
docker compose up
```

## Accessing the Application

Once the containers are running:

| Service    | URL                   |
| ---------- | --------------------- |
| React UI   | http://localhost:3000 |
| Upload API | http://localhost:8000 |

Open your browser and navigate to:

```text
http://localhost:3000
```

From the UI, you can upload PDF files, monitor processing, and download the processed output.

## Upload API

The upload endpoint can also be accessed directly:

```bash
curl --location "http://127.0.0.1:8000/upload" \
--form "file=@/path/to/document.pdf"
```

The API returns a Job ID that is used to track the processing status.

## Docker

All services, including the frontend, are orchestrated using Docker Compose.

To stop the application:

```bash
docker compose down
```

To rebuild images after making changes:

```bash
docker compose up --build
```

## Kubernetes

Kubernetes manifests are available in the `k8s/` directory.

Deploy the application:

```bash
kubectl apply -f k8s/
```

## Future Improvements

* Authentication and authorization
* Real-time job status updates (WebSockets/SSE)
* S3-compatible object storage
* Horizontal scaling of OCR workers
* Monitoring with Prometheus and Grafana
* CI/CD pipeline

## License

This project is intended for learning and experimentation with Rust microservices, asynchronous processing, Docker, Kubernetes, and distributed system design.
