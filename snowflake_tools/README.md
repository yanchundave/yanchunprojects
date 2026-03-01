# Snowflake Data Download CLI

Download Snowflake query results to CSV using key-pair authentication.

## Setup

1. Install dependencies:

```bash
cd snowflake_tools
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Copy your dbt `profiles.yml` into this directory (it is gitignored).

## Usage

```bash
# Inline query (defaults to snowflake-data-analytics profile)
python snowflake_download.py -q "SELECT * FROM my_table LIMIT 100" -o my_data.csv

# Query from a .sql file
python snowflake_download.py -f my_query.sql -o results.csv

# Use a specific profile
python snowflake_download.py -p finance -q "SELECT 1" -o results.csv
```
