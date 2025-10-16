# NYC Mobility – Fullstack (Flask + SQLite + JS)

An end-to-end, enterprise-style fullstack app built on raw NYC taxi trip data.  
I load, clean, enrich, and serve trip-level records, then visualize insights in a web dashboard.

## ✨ Features
- Robust ETL: streaming CSV → cleaning → derived features → SQLite
- Backend API (Flask): KPIs, busiest hours, duration distribution, speed histogram
- Manual algorithms (no built-ins) to satisfy DSA requirement:
  - **Selection sort** for busiest hours ranking
  - **Manual histogram bucketing** for speed distribution
- Frontend: vanilla JS + Chart.js, responsive layout, filters

## 🗂 Project Structure
nyc-mobility/
│
├── app.py # Flask app (routes + API)
├── .env # Environment variables
├── scripts/
│ └── clean_data.py # ETL: load → clean → enrich → insert
│
├── data/
│ ├── raw/ # Raw NYC taxi CSVs
│ ├── processed/ # Cleaned samples
│ └── db/ # SQLite DB lives here
│
├── templates/
│ └── index.html # Dashboard page
│
├── static/
│ └── app.js # Frontend logic + visualizations
│
├── logs/
│ └── cleaning.log # Logs of dropped rows, etc.
│
└── README.md

---

## 🧰 Prerequisites
- Python 3.9+  
- Virtual environment  
- Flask, Pandas, Numpy  

Install dependencies:
```bash
pip install flask pandas numpy

Setup

1. Clone the repo:
git clone https://github.com/sultanhabibllah/nyc-mobility.git
cd nyc-mobility
2. Create .env:
FLASK_DEBUG=1
DATABASE_URL=sqlite:///data/db/nyc_taxi.db
3. Activate virtual environment (Windows):
env\Scripts\activate

Data

Download dataset:
NYC Taxi Trip Duration (Kaggle)

Extract train.zip → data/raw/train_unzipped/train.csv

ETL

Run cleaning + DB load:
python scripts\clean_data.py
This:
- Loads raw data in chunks
- Cleans coordinates and timestamps
- Derives features (trip_speed, trip_distance_km, duration_category)
- Inserts results into nyc_taxi.db

Run the app
python app.py

Open:
http://127.0.0.1:5000

API Endpoints
Endpoint	Description
/	Dashboard UI
/health	App health check
/api/summary	Returns KPIs (trip count, avg speed, etc.)
/api/busiest_hours	Returns top N busiest hours (manual selection sort)
/api/speed_histogram	Returns manual histogram buckets
/api/duration_mix	Returns short/medium/long trip distribution

Derived Features
Feature	Formula	Purpose
trip_distance_km	Haversine formula	Compute distance between pickup/dropoff
trip_speed_kmh	distance ÷ duration	Analyze efficiency
duration_category	Short/Medium/Long thresholds	Show trip length distribution

DB Schema

Table: trips

Column	Type	Example
id	TEXT	id2875421
vendor_id	TEXT	2
pickup_datetime	TEXT	2016-01-01 00:30:55
dropoff_datetime	TEXT	2016-01-01 00:47:58
passenger_count	INTEGER	1
pickup_longitude	REAL	-73.9881
pickup_latitude	REAL	40.7320
dropoff_longitude	REAL	-73.9902
dropoff_latitude	REAL	40.7566
trip_duration	INTEGER	1023
trip_distance_km	REAL	3.28
trip_speed_kmh	REAL	11.5
duration_category	TEXT	Medium
🛠 Troubleshooting
Issue	Fix
Flask not found	Activate env: env\Scripts\activate
No data on dashboard	Run ETL again: python scripts\clean_data.py
“Template not found”	Ensure templates/index.html exists
Chart not loading	Check console (F12) for JS errors

Screenshots
Dashboard Overview
(Insert your screenshot here after running locally)

Video
2-minute walkthrough link (https://drive.google.com/file/d/1Y_MU5lWTtffiJdpHWuE0easmNJEGRDqM/view?usp=sharing)

License
MIT License © 2025 Sultan Habibllah