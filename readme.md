# New World Supermarket Scraper

This project is a supermarket price scraper for collecting and comparing product prices from the "New World" supermarket chain in New Zealand. It includes a Python-based scraper that interacts with New Worlds APIs used for the frontend website and a PostgreSQL database for storing the retrieved data.

## Features

- Scrapes store information and product data from the New World API.
- Stores data in a PostgreSQL database with structured tables for products, stores, prices, and categories.
- Handles authentication and token refresh for API requests.
- Supports multithreaded scraping for efficiency.
- Logs activities and errors for debugging and monitoring.

## Project Structure

### Files

- `init_db.sql`: SQL file to initialize the database schema.
- `config.json`: Configuration file for database and API settings.
- `newworld_scraper.py`: The main Python script for the scraper.

## Requirements

### Software

- Python 3.7+
- PostgreSQL

### Python Dependencies

Install the required Python packages using:

```sh
pip install -r requirements.txt
```

Here are the main libraries used:

- `requests` (for making API requests)
- `psycopg2` (for database connections)
- `tenacity` (for retrying API requests)
- `tqdm` (for progress bars, optional)
- `colorlog` (for colored logs, optional)

## Configuration

Update the `config.json` file with your database and API details:

```json
{
    "db": {
        "host": "127.0.0.1",
        "port": 5432,
        "dbname": "price_comparison",
        "user": "postgres",
        "password": "admin"
    },
    "newWorld": {
        "chainName": "New World",
        "baseUrl": "https://www.newworld.co.nz",
        "apiUrl": "https://api-prod.newworld.co.nz"
    }
}
```

## Usage

1. **Initialize the Database**

     Run the `init_db.sql` file to set up the database schema. Use the `psql` command-line tool or a database GUI:

     ```sh
     psql -U postgres -d price_comparison -f init_db.sql
     ```

2. **Run the Scraper**

     Execute the scraper script:

     ```sh
     python newworld_scraper.py
     ```

3. **Logs**

     Logs are saved to `scraper.log`. Console outputs display `WARNING` and higher levels. Full logs (including `DEBUG`) are stored in the log file.

## How It Works

1. **Load Configuration**: Reads API and database configurations from `config.json`.
2. **Initialize Database**: Ensures the required tables are created.
3. **Authenticate**: Retrieves an API token for authorized requests.
4. **Scrape Stores and Products**:
     - Fetches store details.
     - Iterates through categories to scrape product data.
5. **Store Data**: Saves product, pricing, and category information into the PostgreSQL database.
6. **Error Handling**: Retries transient errors using exponential backoff.

## Contribution

Feel free to contribute by submitting a pull request or reporting issues. Ensure your code follows Python best practices and is well-documented.
