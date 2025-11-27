# 🚀 NASA Data Pipeline 🚀

Automated ETL with GitHub Actions, Python, and Neon PostgreSQL

<p align="center"> <img src="https://upload.wikimedia.org/wikipedia/commons/e/e5/NASA_logo.svg" width="180"> </p>

This project implements a complete ETL pipeline that fetches daily data from multiple NASA API endpoints and stores it in a PostgreSQL database hosted on **Neon.tech**. The pipeline can run locally or automatically through **GitHub Actions**.

---

##  Contents

This project retrieves and stores:

| Data Type | API | Description |
|-----------|-----|-------------|
| **Mars Weather** | InSight Weather API | Atmospheric temperatures, sol keys, UTC dates |
| **Near-Earth Objects (NEO)** | NeoWs API | Asteroid size, hazardous classification, close approach data |
| **Astronomy Picture of the Day (APOD)** | APOD API | Daily image, description, metadata |

Each dataset is stored in a dedicated PostgreSQL table.

---

## Database ER Diagram
erDiagram

    mars_weather {
        INTEGER sol PK
        DATE date
        REAL max_temp 
        REAL min_temp
        REAL avg_temp
        TIMESTAMP DEFAULT NOW() updated 
    }

    space_picture {
        DATE date PK
        TEXT description
        TEXT copyright
        TEXT url
    }

    near_earth_objects {
        INTEGER id PK
        TEXT name 
        REAL min_diameter_meters
        REAL max_diameter_meters 
        BOOLEAN is_potential_hazard
        DATE close_approach_date
        REAL miss_distance_km
        TIMESTAMP DEFAULT NOW() updated
    }




## Environment Variables

These must be stored securely in GitHub Secrets:

| Secret                                 | Name                       |
|----------------------------------------|----------------------------|
| **API_KEY**                            | NASA API key               | 
| **DATABASE_URL**                       | Neon PostgreSQL connection | 

Example value:

postgresql://user:password@host:5432/dbname?sslmode=require

## Running Locally
1. Create .env
API_KEY=your_nasa_key
DATABASE_URL=your_neon_db_url

2. Install dependencies
pip install -r requirements.txt

3. Run ETL
python -m src.main
