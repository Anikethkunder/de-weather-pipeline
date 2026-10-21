# Weather ETL Pipeline (Data Engineering project)

Open-Meteo API -> Python ETL (GitHub Actions, hourly) -> data-quality gate -> SQLite warehouse -> JSON reports.

Stages: extract, transform, quality (gate), load (idempotent upsert), publish.
A failed quality check stops the load, so bad data never reaches the warehouse.

## Setup
1. Create a NEW PUBLIC repo named `de-weather-pipeline` (leave it empty).
2. In a terminal inside THIS folder (must contain pipeline.py):
       git init
       git add .
       git commit -m "Initial commit"
       git branch -M main
       git remote add origin https://github.com/<username>/de-weather-pipeline.git
       git push -u origin main
3. Settings -> Actions -> General -> Workflow permissions -> Read and write -> Save.
4. Actions -> "Weather ETL Pipeline" -> Run workflow.

## Local run (offline fake data)
    PowerShell:  $env:MOCK="1"; python pipeline.py
    Tests:       python -m unittest discover -s tests -v

## Demo failures (Run workflow form)
- inject_fault = extract   -> extract stage fails
- inject_fault = bad_data  -> quality gate fails, load skipped
- hold_seconds = 60        -> run stays "In progress" for a minute
