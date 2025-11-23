# Hotel Reservations Data Analysis & WebUI

A comprehensive data engineering project that performs exploratory data analysis on hotel booking datasets using Apache Spark, populates a PostgreSQL database, and provides a modern web interface for data exploration and filtering.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Technology Stack](#technology-stack)
- [Project Architecture](#project-architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Phase 1: Data Preparation & EDA](#phase-1-data-preparation--eda)
- [Phase 2: Spark Analysis & Database Population](#phase-2-spark-analysis--database-population)
- [Phase 3: WebUI for Searching & Filtering](#phase-3-webui-for-searching--filtering)
- [Project Structure](#project-structure)
- [API Documentation](#api-documentation)
- [Troubleshooting](#troubleshooting)

## Project Overview

This project analyzes hotel reservation data through three progressive phases:

1. **Phase 1**: Data preparation, cleaning, and exploratory data analysis using PySpark
2. **Phase 2**: Advanced analytics with PySpark and database population using PostgreSQL
3. **Phase 3**: Interactive web application for data exploration and filtering

**Datasets:**

- `customer-reservations.csv` - Customer booking data (36,275 rows)
- `hotel-booking.csv` - Hotel booking records (78,703 rows)
- `merged_data.csv` - Unified dataset combining both sources (75,663 rows)

---

## 🛠 Technology Stack

### Backend

- **Apache Spark (PySpark 3.5.0)** - Distributed data processing
- **PostgreSQL 16** - Relational database
- **Flask 3.0.0** - Python web framework
- **psycopg2** - PostgreSQL adapter for Python

### Frontend

- **HTML5/CSS3** - Structure and styling
- **JavaScript (Vanilla)** - Client-side logic
- **Bootstrap 5.1.3** - UI framework
- **Bootstrap Icons** - Icon library

### Infrastructure

- **Docker** - PostgreSQL containerization
- **Python 3.13** - Runtime environment

---

## Project Architecture

```
User Browser
     ↓
Flask Web Server (Port 5001)
     ↓
PostgreSQL Database (Port 5432)
     ↑
PySpark ETL Pipeline
     ↑
CSV Data Files
```

---

## Prerequisites

### Required Software

1. **Python 3.13+**

   ```bash
   python --version
   ```

2. **Docker**

   ```bash
   docker --version
   ```

   If not installed: [https://docs.docker.com/get-started/](https://docs.docker.com/get-started/)

3. **Java 8 or 11** (required for PySpark)
   ```bash
   java -version
   ```

### Python Dependencies

Install all required packages:

```bash
pip install -r requirements.txt
```

**Core dependencies:**

- `pyspark==3.5.0`
- `flask==3.0.0`
- `psycopg2-binary==2.9.9`
- `pandas==2.1.4`
- `numpy==1.26.2`
- `jupyter==1.0.0`
- `matplotlib==3.8.2`
- `seaborn==0.13.0`

---

## Quick Start

### 1. Clone and Setup

```bash
cd cs236_project
source venv/bin/activate  # or use your virtual environment
pip install -r requirements.txt
```

### 2. Start PostgreSQL Database

```bash
cd src/database
bash start_db.sh
```

Verify container is running:

```bash
docker ps | grep hotel_reservations
```

### 3. Load Data into PostgreSQL

```bash
cd src
python scripts/load_data_to_postgres.py
```

This will load all three datasets into PostgreSQL (takes 1-2 minutes).

### 4. Start the Web Application

```bash
cd flask
python run.py
```

### 5. Access the WebUI

Open your browser and navigate to:

```
http://localhost:5001
```

---

## Phase 1: Data Preparation & EDA

### Objectives

- Load and explore hotel booking datasets using PySpark
- Perform data quality checks and exploratory analysis
- Clean and merge datasets into a unified format

### Key Activities

#### 1.1 Environment Setup

- Install Docker for PostgreSQL containerization
- Install PySpark for distributed data processing
- Set up Jupyter notebooks for interactive analysis

#### 1.2 Exploratory Data Analysis (EDA)

Performed comprehensive EDA including:

- **Schema exploration** - Column names, data types, nullable fields
- **Data quality checks** - Row counts, null values, duplicates
- **Statistical analysis** - Distributions, distinct values, outliers
- **Data profiling** - Min/max values, quartiles, standard deviations

#### 1.3 Dataset Processing

**Cleaning Steps:**

- Standardized column names across datasets
- Resolved data type mismatches
- Handled missing values
- Removed duplicates and invalid records

**Merging Process:**

- Identified common columns between datasets
- Aligned schemas to consistent format
- Created unified `merged_data.csv` with key features:
  - `lead_time` - Days between booking and arrival
  - `market_segment_type` - Online/Offline booking channel
  - `avg_price_per_room` - Average nightly rate
  - `booking_status` - 0 (Not Canceled) or 1 (Canceled)
  - `arrival_date` - Guest arrival date
  - `total_stay_nights` - Total duration of stay

### Notebooks

- `src/notebooks/eda.ipynb` - Main exploratory analysis
- `src/notebooks/Phase2_1.ipynb` - Phase 2 Spark analysis
- `src/scripts/eda.py` - Python script version

---

## Phase 2: Spark Analysis & Database Population

### Objectives

- Perform advanced analytics on unified dataset
- Calculate business metrics using PySpark
- Populate PostgreSQL database with all datasets

### 2.1 Spark Analysis

Implemented four key analyses on the unified dataset:

#### 1. Cancellation Rates by Month

```python
# Calculate monthly cancellation percentages
cancellation_rates = mdf_month.groupBy("arrival_month").agg(
    count("*").alias("total_bookings"),
    sum("booking_status").alias("cancellations"),
    round((sum("booking_status") / count("*")) * 100, 2).alias("cancellation_rate")
)
```

**Key Findings:**

- Highest cancellation rate: August (33.41%)
- Lowest cancellation rate: January (12.5%)
- Summer months show significantly higher cancellations

#### 2. Monthly Averages

Computed average price per room and average stay duration for each month to identify pricing trends and booking patterns.

#### 3. Monthly Bookings by Market Segment

```python
# Count bookings by market segment (TA = Travel Agents, TO = Tour Operators)
bookings_by_segment = mdf_month.groupBy("arrival_month", "market_segment_type").count()
```

Analyzed booking distribution across:

- Online bookings
- Offline bookings
- Travel Agents (TA)
- Tour Operators (TO)
- Corporate bookings

#### 4. Seasonality Analysis

Identified peak revenue months by calculating:

```python
monthly_revenue = avg_price_per_room × total_bookings
```

Determined the most popular booking month based on revenue generation.

### 2.2 Database Population

#### Start PostgreSQL Container

```bash
docker run -d \
  --name hotel_reservations \
  -e POSTGRES_USER=cs236_user \
  -e POSTGRES_PASSWORD=cs236_pass \
  -e POSTGRES_DB=hotel_reservations \
  -p 5432:5432 \
  postgres:16
```

#### Load Data with PySpark

```bash
cd src
python scripts/load_data_to_postgres.py
```

This script:

1. Creates Spark session with PostgreSQL JDBC driver
2. Reads CSV files with schema inference
3. Writes data to three PostgreSQL tables:
   - `customer_reservations` (36,275 rows)
   - `hotel_booking` (78,703 rows)
   - `merged_dataset` (75,663 rows)
4. Verifies data integrity

#### Database Schema

**customer_reservations:**

- Booking_ID (text)
- stays_in_weekend_nights (integer)
- stays_in_week_nights (integer)
- lead_time (integer)
- arrival_year (integer)
- arrival_month (integer)
- arrival_date (date)
- market_segment_type (text)
- avg_price_per_room (numeric)
- booking_status (integer)

**hotel_booking:**

- hotel (text)
- booking_status (integer)
- lead_time (integer)
- arrival_year (integer)
- arrival_month (integer)
- arrival_date_week_number (integer)
- arrival_date_day_of_month (integer)
- stays_in_weekend_nights (integer)
- stays_in_week_nights (integer)
- market_segment_type (text)
- country (text)
- avg_price_per_room (numeric)
- email (text)

**merged_dataset:**

- lead_time (integer)
- market_segment_type (text)
- avg_price_per_room (numeric)
- booking_status (integer)
- arrival_date (date)
- total_stay_nights (integer)

#### Connection Configuration

Database settings are configured in `flask/config.py`:

```python
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "hotel_reservations"
DB_USER = "cs236_user"
DB_PASSWORD = "cs236_pass"
```

---

## Phase 3: WebUI for Searching & Filtering

### Objectives

- Build interactive web interface for data exploration
- Enable dynamic filtering on any column
- Provide intuitive data visualization

### Features

#### Dataset Selection

- Dropdown menu to select from three available datasets
- Automatic column detection for selected dataset
- Real-time schema adaptation

#### Dynamic Filtering System

- **Add Multiple Filters** - Combine conditions with AND logic
- **Filter Operators:**
  - `equals` - Exact match
  - `contains` - Text substring match (case-insensitive)
  - `greater` - Numeric greater than
  - `less` - Numeric less than
- **Remove Filters** - Easily delete unwanted conditions
- **Clear All** - Reset filters and start over

### Application Structure

```
flask/
├── app/
│   ├── __init__.py           # Flask app factory
│   ├── routes.py             # API endpoints
│   ├── static/
│   │   ├── app.js           # Frontend JavaScript
│   │   └── style.css        # Custom styling
│   └── templates/
│       ├── base.html        # Base template
│       └── index.html       # Main page
├── config.py                 # Database configuration
└── run.py                    # Application entry point
```

### Backend API Endpoints

#### GET `/`

Returns the main web interface.

#### GET `/datasets`

**Response:**

```json
{
  "datasets": ["customer_reservations", "hotel_booking", "merged_dataset"]
}
```

#### GET `/columns/<dataset>`

**Response:**

```json
{
  "columns": ["lead_time", "market_segment_type", "avg_price_per_room", ...]
}
```

#### POST `/query`

**Request Body:**

```json
{
  "dataset": "merged_dataset",
  "filters": [
    {
      "column": "avg_price_per_room",
      "operator": "less",
      "value": "100"
    },
    {
      "column": "booking_status",
      "operator": "equals",
      "value": "0"
    }
  ],
  "limit": 50,
  "page": 1
}
```

**Response:**

```json
{
  "success": true,
  "data": [
    {
      "lead_time": 56,
      "market_segment_type": "Online",
      "avg_price_per_room": 88.0,
      "booking_status": 0,
      "arrival_date": "2018-12-08",
      "total_stay_nights": 3
    }
  ],
  "columns": ["lead_time", "market_segment_type", ...],
  "count": 50
}
```

### Security Features

- **SQL Injection Protection** - Parameterized queries
- **Input Validation** - Server-side validation of all inputs
- **Error Handling** - Graceful error messages
- **CORS Protection** - Same-origin policy enforcement

---

## Project Structure

```
cs236_project/
├── flask/                          # Web application
│   ├── app/
│   │   ├── __init__.py            # Flask app initialization
│   │   ├── routes.py              # API routes and database logic
│   │   ├── static/
│   │   │   ├── app.js            # Frontend JavaScript (SPA logic)
│   │   │   └── style.css         # Custom CSS styling
│   │   └── templates/
│   │       ├── base.html         # Base HTML template
│   │       ├── index.html        # Main page
│   │       └── results.html      # Results page (unused)
│   ├── config.py                  # Database configuration
│   ├── run.py                     # Flask application entry point
│   └── check_setup.py             # Setup validation script
│
├── src/                           # Data processing & analysis
│   ├── data/                      # Raw CSV datasets
│   │   ├── customer-reservations.csv
│   │   ├── hotel-booking.csv
│   │   ├── merged_data.csv
│   │   └── schema.sql
│   ├── notebooks/                 # Jupyter notebooks for analysis
│   │   ├── eda.ipynb             # Exploratory data analysis
│   │   ├── Phase2_1.ipynb        # Phase 2 Spark analysis
│   │   └── README.md
│   ├── scripts/                   # Python scripts
│   │   ├── eda.py                # EDA script version
│   │   ├── load_data_to_postgres.py  # Database population
│   │   └── README.md
│   └── database/                  # Database setup files
│       ├── postgresql-42.7.3.jar # PostgreSQL JDBC driver
│       ├── start_db.sh           # Docker startup script
│       └── README.md
│
├── venv/                          # Python virtual environment
├── requirements.txt               # Python dependencies
├── reorganize_project.py          # Project reorganization script
└── README.md                      # This file
```

> **Note**: If your project isn't organized yet, run `python reorganize_project.py` to automatically organize all files into this structure.

---

## API Documentation

### Database Connection

```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="hotel_reservations",
    user="cs236_user",
    password="cs236_pass"
)
```

### Example Queries

#### Get all bookings under $100

```sql
SELECT * FROM merged_dataset
WHERE avg_price_per_room < 100
LIMIT 50;
```

#### Get online bookings that weren't canceled

```sql
SELECT * FROM merged_dataset
WHERE market_segment_type = 'Online'
  AND booking_status = 0;
```

#### Calculate average price by month

```sql
SELECT
    EXTRACT(MONTH FROM arrival_date) as month,
    AVG(avg_price_per_room) as avg_price,
    COUNT(*) as total_bookings
FROM merged_dataset
GROUP BY EXTRACT(MONTH FROM arrival_date)
ORDER BY month;
```

---

## 🧪 Testing the Application

### 1. Verify Database Connection

```bash
docker exec hotel_reservations psql -U cs236_user -d hotel_reservations -c "\dt"
```

Expected output:

```
                List of relations
 Schema |         Name          | Type  |   Owner
--------+-----------------------+-------+------------
 public | customer_reservations | table | cs236_user
 public | hotel_booking         | table | cs236_user
 public | merged_dataset        | table | cs236_user
```

### 2. Test API Endpoints

```bash
# Test datasets endpoint
curl http://localhost:5001/datasets

# Test columns endpoint
curl http://localhost:5001/columns/merged_dataset

# Test query endpoint
curl -X POST http://localhost:5001/query \
  -H "Content-Type: application/json" \
  -d '{
    "dataset": "merged_dataset",
    "filters": [{"column": "booking_status", "operator": "equals", "value": "0"}],
    "limit": 10,
    "page": 1
  }'
```

### 3. Test UI Interactions

1. **Select Dataset**: Choose "Merged Dataset" from dropdown
2. **Add Filter**: Click "+ Add Filter"
3. **Set Conditions**:
   - Column: `avg_price_per_room`
   - Operator: `less`
   - Value: `100`
4. **Execute Query**: Click "Execute Query" button
5. **Navigate Results**: Use pagination controls

---

## Troubleshooting

### Issue: Port 5001 Already in Use

**Error:**

```
Address already in use - Port 5001
```

**Solution:**

```bash
# Find process using port 5001
lsof -i :5001

# Kill the process
kill -9 <PID>

# Or use a different port in run.py
app.run(debug=True, host="0.0.0.0", port=5002)
```

### Issue: PostgreSQL Container Won't Start

**Error:**

```
docker: Error response from daemon: port is already allocated
```

**Solution:**

```bash
# Stop existing container
docker stop hotel_reservations

# Remove existing container
docker rm hotel_reservations

# Start fresh
cd src/database && bash start_db.sh
```

### Issue: No Datasets Appear in Dropdown

**Cause:** Database is empty.

**Solution:**

```bash
# Load data into PostgreSQL
cd src
python scripts/load_data_to_postgres.py
```

### Issue: Flask Import Error

**Error:**

```
ModuleNotFoundError: No module named 'flask'
```

**Solution:**

```bash
pip install flask psycopg2-binary
```

### Issue: PySpark Java Error

**Error:**

```
Exception: Java gateway process exited before sending its port number
```

**Solution:**

```bash
# Install Java 8 or 11
brew install openjdk@11

# Set JAVA_HOME
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```

### Issue: Database Connection Failed

**Error:**

```
psycopg2.OperationalError: could not connect to server
```

**Solution:**

```bash
# Check if container is running
docker ps | grep hotel_reservations

# Check container logs
docker logs hotel_reservations

# Verify connection settings in config.py match start_db.sh
```

---

## Sample Queries to Try

### Query 1: Affordable Long-Lead Bookings

- Dataset: `merged_dataset`
- Filter 1: `lead_time` > `100`
- Filter 2: `avg_price_per_room` < `100`
- Filter 3: `booking_status` = `0`

### Query 2: Summer Weekend Stays

- Dataset: `customer_reservations`
- Filter 1: `arrival_month` = `7`
- Filter 2: `stays_in_weekend_nights` > `0`

### Query 3: High-Value Bookings

- Dataset: `merged_dataset`
- Filter 1: `avg_price_per_room` > `200`
- Filter 2: `total_stay_nights` > `3`

### Query 4: Online Market Segment

- Dataset: `merged_dataset`
- Filter 1: `market_segment_type` = `Online`
- Filter 2: `booking_status` = `0`

## License

This project is part of an academic assignment for CS236.

---

## Acknowledgments

- Apache Spark for distributed data processing
- PostgreSQL for robust data storage
- Flask for lightweight web framework
- Bootstrap for beautiful UI components

**Built with ❤️ for CS236 - Big Data Systems**
