# Weather ETL Pipeline - AWS

Automated ETL pipeline for extracting, transforming, and loading daily weather data for Islamabad (Pakistan) using AWS services.

## Architecture
OpenWeatherMap API → AWS Glue (PySpark) → Amazon S3 → Amazon Athena → Tableau


Copy code

## Tech Stack

- AWS Glue (PySpark)
- Amazon S3
- Amazon Athena
- EventBridge Scheduler
- CloudWatch
- Tableau Public

## Features

- Automated daily weather data extraction
- PySpark for distributed data processing
- Scheduled execution (8:00 AM PKT daily)
- Cloud monitoring with alerts
- SQL querying via Athena
- Interactive visualizations

## Project Structure
. ├── README.md ├── etl/ │ └── weather_etl.py ├── sql/ │ └── athena_queries.sql └── visualizations/ ├── temp_trend.png └── weather_categories.png


Copy code

## Schedule

- **Frequency**: Daily at 8:00 AM Pakistan Standard Time
- **Scheduler**: EventBridge Scheduler

## Sample Queries (Athena)

```sql
SELECT * FROM weather_db.weather ORDER BY date DESC LIMIT 10;
SELECT AVG(temp) FROM weather_db.weather;
