## SEC 13F Filings Polling

### Overview
Monitors and processes SEC 13F-HR and 13F-HR/A filings in real-time, extracting institutional investment data and storing it in a PostgreSQL database. The system watches for new filings, processes relevant ones based on a configurable CIK list, and maintains persistent storage of all processed filings.

### Key Features
- Polling for new 13F filings
- Dynamic CIK list management with file watching for changes
- Processing with rate limiting and circuit breakers
- Database integration for persistent storage of filings and holdings
- Health monitoring with optional healthcheck pings
- Efficient processing using batch inserts and caching

### Prerequisite
- Python 3.11.4
- Postgres Database
- Edgar API

## Installation
Clone repository
```
git clone https://github.com/JasonLing95/secpoll.git
cd secpoll
```

Install dependencies
```
pip install -r requirements.txt
```

Create Environments
```
SEC_IDENTITY="example.email@example.com"  # Required by SEC
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="postgres"
DB_PASSWORD="yourpassword"
DB_NAME="sec"
CIK_FILE="ciks.txt"  # Path to your CIK list
HEALTHCHECK_URL="https://hc-ping.com/your-uuid"  # Optional
```

### Usage
```
python main.py --cik path/to/ciks.txt
```

Build the Docker image
```
docker build -t secpoll:1.0 .
```

Run on Docker (on Docker network "common-net")
```
docker run -d --network=common-net -v /app/logs:/app/logs -v /app/cik:/app/cik -e DB_HOST=sec-database -e DB_PASSWORD=password -e DB_PORT=5432 -e DB_NAME=sec secpoll:1.0 --cik /app/cik/ciks.txt
```