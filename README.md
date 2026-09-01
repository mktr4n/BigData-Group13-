# Group 13 - Big Data Assignment

## Prerequisites
- Docker Desktop
- Git

## Getting Started
1. Clone this repository.
2. Download the project dataset files and extract them to the `data/` folder:
   - `financial_data.archive.gz`
   - `enheter_alle.json.gz`
3. Start the Docker containers:
   ```bash
   docker compose up -d
   ```
4. Restore the MongoDB financial data:
   ```bash
   docker exec group13_mongodb mongorestore --gzip --archive=/import/financial_data.archive.gz --drop
   ```
