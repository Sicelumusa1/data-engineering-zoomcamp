# data-engineering-zoomcamp
My journey to the backend of the data ecosystem that involves expanding from general backend development to the specialized field of data engineering, where the focus shifts to building and maintaining the infrastructure and pipelines for managing and analyzing large amounts of data.

# Data Engineering Zoomcamp – Week 1 (Ingestion)

This repository contains the code and configuration for **Week 1** of the Data Engineering Zoomcamp.

The focus for this week was **ingestion as a system**, using containerization and reproducible environments.

---

## What this repo includes

### Key Components
- **Python ingestion script** using `pandas` (chunked CSV ingestion)
- **Dockerized PostgreSQL** database
- **Dockerized pgAdmin** for querying and inspecting data
- **Docker Compose** to orchestrate the services
- **Terraform + GCP setup** for cloud infrastructure (initial setup)

---

## Tech Stack
- Python + pandas
- PostgreSQL
- Docker
- Docker Compose
- Terraform
- Google Cloud Platform (GCP)

---

## How to run locally

1. Install Docker
2. Build ingest_data and zones_data images (taxi_ingest:v001 and zones:v001) 
3. Run:

```bash
docker compose up
