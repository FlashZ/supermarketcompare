#!/usr/bin/env python3

import os
import json
import time
import datetime
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional
from contextlib import contextmanager
import threading
import requests
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional: For colored logs and progress bars
try:
    import colorlog
except ImportError:
    colorlog = None

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ------------------------------------------------------------------
# Configure Logging
# ------------------------------------------------------------------
def setup_logging():
    """
    Configures logging to output to both a file and the terminal.
    - File: Records all log levels with rotation.
    - Console: Displays INFO and above levels, optionally with colors.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Capture all log levels

    # Rotating File Handler
    file_handler = RotatingFileHandler('scraper.log', maxBytes=5*1024*1024, backupCount=5)
    file_handler.setLevel(logging.DEBUG)  # Log all levels to file

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)  # Log WARNING and above to console

    # Formatter
    if colorlog:
        # Colored Formatter for console
        formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )
    else:
        # Standard Formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Apply formatters to handlers
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# ------------------------------------------------------------------
# 1) Load Config
# ------------------------------------------------------------------
def load_config() -> Dict:
    """
    Loads configuration from config.json located in the same directory as the script.
    """
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, "config.json")
    if not os.path.exists(config_path):
        logging.critical("config.json not found.")
        raise FileNotFoundError("config.json not found.")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

# Initialize Config
CONFIG = load_config()

DB_HOST = CONFIG["db"]["host"]
DB_PORT = CONFIG["db"]["port"]
DB_NAME = CONFIG["db"]["dbname"]
DB_USER = CONFIG["db"]["user"]
DB_PASSWORD = CONFIG["db"]["password"]

CHAIN_NAME = CONFIG["newWorld"]["chainName"]
NEW_WORLD_BASE_URL = CONFIG["newWorld"]["baseUrl"]
API_BASE_URL = CONFIG["newWorld"]["apiUrl"]

# Limit the scraper to the first 5 stores
MAX_STORES = 5

# ------------------------------------------------------------------
# 2) Initialize DB (Relational + cost-per-unit columns)
# ------------------------------------------------------------------
def init_db(connection_pool: pool.ThreadedConnectionPool) -> None:
    """
    Initializes the database tables required for the scraper.
    """
    with get_db_connection(connection_pool) as conn:
        try:
            with conn.cursor() as cur:
                # master_products
                cur.execute("""
                CREATE TABLE IF NOT EXISTS master_products (
                  id SERIAL PRIMARY KEY,
                  barcode TEXT UNIQUE,
                  brand TEXT,
                  product_name TEXT NOT NULL,
                  package_size TEXT,
                  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                  updated_at TIMESTAMP
                );
                """)

                # product_aliases
                cur.execute("""
                CREATE TABLE IF NOT EXISTS product_aliases (
                  id SERIAL PRIMARY KEY,
                  master_product_id INT NOT NULL REFERENCES master_products(id),
                  retailer_name TEXT NOT NULL,
                  retailer_sku TEXT NOT NULL,
                  display_name TEXT,
                  extra_json JSONB,
                  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                  updated_at TIMESTAMP,
                  UNIQUE (retailer_name, retailer_sku)
                );
                """)

                # stores
                cur.execute("""
                CREATE TABLE IF NOT EXISTS stores (
                  id SERIAL PRIMARY KEY,
                  retailer_name TEXT NOT NULL,
                  store_name TEXT,
                  retailer_store_id TEXT UNIQUE,
                  address TEXT,
                  latitude NUMERIC(9,6),
                  longitude NUMERIC(9,6),
                  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                  updated_at TIMESTAMP
                );
                """)

                # prices (including cost-per-unit columns)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS prices (
                  id SERIAL PRIMARY KEY,
                  master_product_id INT NOT NULL REFERENCES master_products(id),
                  store_id INT NOT NULL REFERENCES stores(id),

                  price NUMERIC(10,2) NOT NULL,
                  non_loyalty_price NUMERIC(10,2),
                  promo_price NUMERIC(10,2),
                  promo_data JSONB,

                  unit_price NUMERIC(10,2),
                  unit_quantity NUMERIC(10,2),
                  unit_measure TEXT,

                  scraped_at TIMESTAMP NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """)

                # categories
                cur.execute("""
                CREATE TABLE IF NOT EXISTS categories (
                  id SERIAL PRIMARY KEY,
                  name TEXT NOT NULL,
                  parent_id INT REFERENCES categories(id),
                  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                  updated_at TIMESTAMP,
                  UNIQUE (name, parent_id)
                );
                """)

                # product_categories
                cur.execute("""
                CREATE TABLE IF NOT EXISTS product_categories (
                  product_alias_id INT NOT NULL REFERENCES product_aliases(id),
                  category_id INT NOT NULL REFERENCES categories(id),
                  PRIMARY KEY (product_alias_id, category_id)
                );
                """)

                # nutrition
                cur.execute("""
                CREATE TABLE IF NOT EXISTS product_nutrition (
                  id SERIAL PRIMARY KEY,
                  master_product_id INT NOT NULL REFERENCES master_products(id),
                  nutrient_code TEXT,
                  nutrient_name TEXT NOT NULL,
                  amount NUMERIC(10,2),
                  unit TEXT,
                  basis_qty NUMERIC(10,2),
                  measurement_precision TEXT,
                  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                  updated_at TIMESTAMP
                );
                """)

                # Ensure 'unit_price', 'unit_quantity', and 'unit_measure' columns exist
                cur.execute("""
                ALTER TABLE prices
                ADD COLUMN IF NOT EXISTS unit_price NUMERIC(10,2);
                """)
                cur.execute("""
                ALTER TABLE prices
                ADD COLUMN IF NOT EXISTS unit_quantity NUMERIC(10,2);
                """)
                cur.execute("""
                ALTER TABLE prices
                ADD COLUMN IF NOT EXISTS unit_measure TEXT;
                """)

                # Attempt Timescale creation
                try:
                    cur.execute("""
                        CREATE EXTENSION IF NOT EXISTS timescaledb;
                        SELECT create_hypertable('prices', 'scraped_at', if_not_exists => TRUE);
                    """)
                except Exception as e:
                    logging.warning(f"Timescale hypertable error: {e}")

                conn.commit()
                logging.info("[init_db] Database initialized successfully.")
        except Exception as e:
            conn.rollback()
            logging.critical(f"[init_db ERROR] {e}")
            raise

# ------------------------------------------------------------------
# 3) Example cookies / headers (replace with your real data)
# ------------------------------------------------------------------
# Initial cookies (replace with real cookies as needed)
INITIAL_COOKIES = {
    "shell#lang": "en",
    "SessionCookieIdV3": "d0b8a5de92b241dbab19eeb1dfe6569a",
    "ASP.NET_SessionId": "ewl4cstvwzyuhluhaajrh0sx",
    # ... other cookies
    # e.g., "server_nearest_store_v2": "...",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://www.newworld.co.nz",
    "Accept-Language": "en-NZ,en-US;q=0.9,en;q=0.8"
    # Add more headers if needed (from the request details)
}

# ------------------------------------------------------------------
# 4) Authentication Handling with Token Refresh
# ------------------------------------------------------------------
class TokenManager:
    """
    Manages the authentication token, ensuring it's refreshed when expired.
    """
    def __init__(self, session: requests.Session):
        self.session = session
        self.token = ""
        self.lock = threading.Lock()

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
    def get_auth_token(self) -> str:
        """
        Retrieves the authentication token by making a GET request to the account endpoint.
        Retries on transient network errors.
        """
        url = f"{NEW_WORLD_BASE_URL}/CommonApi/Account/GetCurrentUser"
        try:
            logging.info(f"[TokenManager] Making request to {url} for auth token.")
            resp = self.session.get(url, headers=HEADERS, timeout=10)
            logging.info(f"[TokenManager] Auth token response status: {resp.status_code}")
            resp.raise_for_status()  # raises HTTPError for bad responses
            data = resp.json()
            logging.debug(f"[TokenManager DEBUG] Auth response data: {json.dumps(data, indent=2)}")
            token = data.get("access_token")
            if not token:
                logging.error("[TokenManager] No access_token found in auth response.")
                raise ValueError("No access_token in response.")
            logging.info("[TokenManager] Authentication token retrieved successfully.")
            return token
        except requests.HTTPError as e:
            logging.error(f"[TokenManager ERROR] Failed to get auth token: {e}")
            raise
        except Exception as e:
            logging.error(f"[TokenManager ERROR] Unexpected error while getting auth token: {e}")
            raise

    def refresh_token(self) -> None:
        """
        Refreshes the authentication token in a thread-safe manner.
        """
        with self.lock:
            logging.info("[TokenManager] Refreshing authentication token...")
            self.token = self.get_auth_token()
            logging.debug(f"[TokenManager DEBUG] New token: {self.token[:30]}...")

    def get_valid_token(self) -> str:
        """
        Returns a valid token, refreshing it if necessary.
        """
        if not self.token:
            self.refresh_token()
        return self.token

# ------------------------------------------------------------------
# 5) Store Upsert
# ------------------------------------------------------------------
@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(psycopg2.OperationalError)
)
def get_stores(session: requests.Session, token: str) -> List[Dict]:
    """
    Fetches the list of stores from the API.
    """
    url = f"{NEW_WORLD_BASE_URL}/BrandsApi/BrandsStore/GetBrandStores"
    headers = dict(HEADERS)
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        logging.info(f"[get_stores] Making request to {url}")
        resp = session.get(url, headers=headers, timeout=10)
        logging.info(f"[get_stores] Response status: {resp.status_code}")
        resp.raise_for_status()
        stores = resp.json()
        logging.info(f"[get_stores] Retrieved {len(stores)} stores.")
        return stores
    except requests.HTTPError as e:
        logging.error(f"[get_stores ERROR] Failed to get stores: {e}")
        raise
    except Exception as e:
        logging.error(f"[get_stores ERROR] Unexpected error while getting stores: {e}")
        raise

def upsert_store(connection_pool: pool.ThreadedConnectionPool, store_obj: Dict) -> int:
    """
    Inserts or updates a store in the database and returns its ID.
    """
    ecom_id = store_obj.get("EcomStoreId")
    name = store_obj.get("name", "")
    logging.debug(f"[upsert_store DEBUG] Processing store '{name}' with EcomStoreId: {ecom_id}")

    with get_db_connection(connection_pool) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO stores (retailer_name, store_name, retailer_store_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (retailer_store_id)
                DO UPDATE SET store_name = EXCLUDED.store_name, updated_at = NOW()
                RETURNING id;
                """, (CHAIN_NAME, name, ecom_id))
                store_id = cur.fetchone()[0]
                logging.info(f"[upsert_store] Store '{name}' upserted with ID {store_id}.")
                conn.commit()
                return store_id
        except Exception as e:
            conn.rollback()
            logging.error(f"[upsert_store ERROR] Failed to upsert store '{name}': {e}")
            raise

# ------------------------------------------------------------------
# 6) Paginated Products
# ------------------------------------------------------------------
@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def get_paginated_products(session: requests.Session, token_manager: TokenManager, store_id: str, category_path: List[str], page: int=0, hits_per_page: int=50) -> Dict:
    """
    Retrieves a page of products from the API based on the category path.
    Retries on transient network errors.
    """
    url = f"{API_BASE_URL}/v1/edge/search/paginated/products"

    headers = dict(HEADERS)
    token = token_manager.get_valid_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    # Construct the category filters based on depth
    category_filters = [f'category{i}NI:"{cat}"' for i, cat in enumerate(category_path)]
    filter_str = f"stores:{store_id} AND " + " AND ".join(category_filters)

    payload = {
        "algoliaQuery": {
            "attributesToHighlight": [],
            "attributesToRetrieve": [
                "productID",
                "Type",
                "sponsored",
                "category0NI",
                "category1NI",
                "category2NI"
            ],
            "facets": [
                "brand",
                "category1NI",
                "onPromotion",
                "productFacets"
            ],
            "filters": filter_str,
            "highlightPostTag": "__/ais-highlight__",
            "highlightPreTag": "__ais-highlight__",
            "page": page,
            "hitsPerPage": hits_per_page,
            "maxValuesPerFacet": 100
        },
        "storeId": store_id,
        "algoliaFacetQueries": [],
        "precisionMedia": {
            "adDomain": "CATEGORY_PAGE",
            "adPositions": [4, 8, 12, 16],
            "disableAds": True,
            "publishImpressionEvent": False
        },
        "sortOrder": "NI_POPULARITY_ASC",
        "tobaccoQuery": False,
        "hitsPerPage": hits_per_page,
        "page": page
    }

    try:
        logging.info(f"[get_paginated_products] Making request to {url} for category '{' > '.join(category_path)}', page {page + 1}")
        resp = session.post(url, headers=headers, json=payload, timeout=10)
        logging.info(f"[get_paginated_products] Response status: {resp.status_code}")

        if resp.status_code == 401:
            logging.warning(f"[get_paginated_products] Unauthorized access detected. Refreshing token and retrying...")
            token_manager.refresh_token()
            headers["Authorization"] = f"Bearer {token_manager.get_valid_token()}"
            resp = session.post(url, headers=headers, json=payload, timeout=10)
            logging.info(f"[get_paginated_products] Retry response status: {resp.status_code}")
            resp.raise_for_status()

        elif resp.status_code != 200:
            logging.error(f"[get_paginated_products] Error response body: {resp.text}")

        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            logging.error(f"[get_paginated_products ERROR] Expected dict for paginated products, got {type(data)}")
            logging.debug(f"[get_paginated_products DEBUG] Response content: {data}")
            return {}

        # Debugging: Print full response if no products found
        products = data.get("products", [])
        if not products:
            logging.debug(f"[get_paginated_products DEBUG] No products found. Full Response:")
            logging.debug(json.dumps(data, indent=2))

        return data

    except requests.exceptions.HTTPError as e:
        logging.error(f"[get_paginated_products ERROR] Request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"[get_paginated_products ERROR] Response content: {e.response.text}")
        else:
            logging.error("[get_paginated_products ERROR] No response content available.")
        raise

def get_all_products_for_store(session: requests.Session, token_manager: TokenManager, store_id: str, category_path: List[str]) -> List[Dict]:
    """
    Retrieves all product summaries for a given store and category by iterating through paginated results.
    """
    all_items = []
    page = 0
    hits_per_page = 50  # Number of items per request
    while True:
        try:
            data = get_paginated_products(session, token_manager, store_id, category_path=category_path, page=page, hits_per_page=hits_per_page)
            products = data.get("products", [])
            if not products:
                logging.info(f"[get_all_products_for_store] No products found on page {page + 1} for category '{' > '.join(category_path)}'. Ending pagination.")
                break

            all_items.extend(products)

            # The response should include "page" and "totalPages"
            current_page = data.get("page", page)
            total_pages = data.get("totalPages", 1)

            logging.info(f"[get_all_products_for_store] Fetched {len(products)} items on page {current_page + 1}/{total_pages} for category '{' > '.join(category_path)}'")

            if current_page >= (total_pages - 1):
                break
            page += 1
            time.sleep(0.2)  # Adjust based on API rate limits
        except requests.HTTPError as e:
            logging.error(f"[get_all_products_for_store ERROR] HTTP error on page {page + 1} for category '{' > '.join(category_path)}': {e}")
            break
        except Exception as e:
            logging.error(f"[get_all_products_for_store ERROR] Unexpected error on page {page + 1} for category '{' > '.join(category_path)}': {e}")
            break

    return all_items

# ------------------------------------------------------------------
# 7) Detailed Product
# ------------------------------------------------------------------
@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def get_product_detail(session: requests.Session, token_manager: TokenManager, store_id: str, product_id: str) -> Dict:
    """
    Fetches detailed information for a specific product.
    Retries on transient network errors.
    """
    url = f"{API_BASE_URL}/v1/edge/store/{store_id}/product/{product_id}"
    headers = dict(HEADERS)
    token = token_manager.get_valid_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        logging.info(f"[get_product_detail] Making request to {url}")
        resp = session.get(url, headers=headers, timeout=10)
        logging.info(f"[get_product_detail] Response status: {resp.status_code}")

        if resp.status_code == 401:
            logging.warning(f"[get_product_detail] Unauthorized access detected. Refreshing token and retrying...")
            token_manager.refresh_token()
            headers["Authorization"] = f"Bearer {token_manager.get_valid_token()}"
            resp = session.get(url, headers=headers, timeout=10)
            logging.info(f"[get_product_detail] Retry response status: {resp.status_code}")

        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            logging.error(f"[get_product_detail ERROR] Expected dict for product detail, got {type(data)} for product ID {product_id}")
            return {}
        logging.info(f"[get_product_detail] Retrieved details for product ID {product_id}.")
        return data
    except requests.HTTPError as e:
        logging.error(f"[get_product_detail ERROR] Failed to fetch product detail for {product_id}: {e}")
        raise
    except ValueError:
        logging.error(f"[get_product_detail ERROR] Failed to parse JSON for product detail of {product_id}. Response was not valid JSON.")
        raise
    except Exception as e:
        logging.error(f"[get_product_detail ERROR] Unexpected error while fetching product detail for {product_id}: {e}")
        raise

# ------------------------------------------------------------------
# 8) Category Bridging (Upsert with Hierarchy)
# ------------------------------------------------------------------
def extract_leaf_categories(categories: List[Dict], parent_path: Optional[List[str]]=None) -> List[List[str]]:
    """
    Recursively extracts all leaf subcategories from the category tree.

    Args:
        categories (list): List of category dictionaries.
        parent_path (list): Accumulated path of category names.

    Returns:
        list of lists: Each inner list represents the path to a leaf subcategory.
    """
    if parent_path is None:
        parent_path = []

    leaf_categories = []

    for category in categories:
        current_path = parent_path + [category['name']]
        children = category.get('children', [])
        if children:
            leaf_categories.extend(extract_leaf_categories(children, current_path))
        else:
            leaf_categories.append(current_path)

    return leaf_categories

def upsert_category(connection_pool: pool.ThreadedConnectionPool, category_path: List[str]) -> Optional[int]:
    """
    Inserts or updates a category hierarchy and returns the leaf category ID.

    Args:
        connection_pool: Database connection pool.
        category_path (list): List of category names representing the path.

    Returns:
        int: ID of the leaf category or None if failed.
    """
    parent_id = None
    with get_db_connection(connection_pool) as conn:
        try:
            with conn.cursor() as cur:
                for cat_name in category_path:
                    cat_name = cat_name.strip()
                    if not cat_name:
                        continue
                    cur.execute("""
                    INSERT INTO categories (name, parent_id)
                    VALUES (%s, %s)
                    ON CONFLICT (name, parent_id)
                    DO UPDATE SET updated_at = NOW()
                    RETURNING id;
                    """, (cat_name, parent_id))
                    result = cur.fetchone()
                    if result:
                        parent_id = result[0]
                    else:
                        # Fetch existing category ID
                        if parent_id:
                            cur.execute("SELECT id FROM categories WHERE name = %s AND parent_id = %s", (cat_name, parent_id))
                        else:
                            cur.execute("SELECT id FROM categories WHERE name = %s AND parent_id IS NULL", (cat_name,))
                        fetched = cur.fetchone()
                        parent_id = fetched[0] if fetched else None
            conn.commit()
            return parent_id
        except Exception as e:
            conn.rollback()
            logging.error(f"[upsert_category ERROR] Failed to upsert category '{' > '.join(category_path)}': {e}")
            return None

def link_product_to_category(connection_pool: pool.ThreadedConnectionPool, product_alias_id: int, category_id: int) -> None:
    """
    Links a product alias to a category in the database.

    Args:
        connection_pool: Database connection pool.
        product_alias_id (int): ID of the product alias.
        category_id (int): ID of the category.
    """
    if not category_id:
        return
    with get_db_connection(connection_pool) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO product_categories (product_alias_id, category_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """, (product_alias_id, category_id))
            conn.commit()
            logging.info(f"[link_product_to_category] Linked product_alias_id {product_alias_id} to category_id {category_id}.")
        except Exception as e:
            conn.rollback()
            logging.error(f"[link_product_to_category ERROR] Failed to link product_alias_id {product_alias_id} to category_id {category_id}: {e}")

# ------------------------------------------------------------------
# 9) Nutrition
# ------------------------------------------------------------------
def insert_nutrition(connection_pool: pool.ThreadedConnectionPool, master_product_id: int, nut_data: Dict) -> None:
    """
    Inserts nutritional information for a product.

    Args:
        connection_pool: Database connection pool.
        master_product_id (int): ID of the master product.
        nut_data (dict): Nutrient information.
    """
    code = nut_data.get("nutrientType", "")
    name = nut_data.get("nutrientTypeDescription", "")
    amount = nut_data.get("qtyContained", 0)
    uom = nut_data.get("nutrientUom", "")
    basis = nut_data.get("nutrientBasisQty", 100)
    precision = nut_data.get("measurementPrecision", "")

    with get_db_connection(connection_pool) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO product_nutrition (
                  master_product_id,
                  nutrient_code,
                  nutrient_name,
                  amount,
                  unit,
                  basis_qty,
                  measurement_precision
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    master_product_id,
                    code,
                    name,
                    amount,
                    uom,
                    basis,
                    precision
                ))
            conn.commit()
            logging.info(f"[insert_nutrition] Inserted nutrition for master_product_id {master_product_id}.")
        except Exception as e:
            conn.rollback()
            logging.error(f"[insert_nutrition ERROR] Failed to insert nutrition for master_product_id {master_product_id}: {e}")

def clear_nutrition_for_product(connection_pool: pool.ThreadedConnectionPool, master_product_id: int) -> None:
    """
    Clears existing nutritional information for a product.

    Args:
        connection_pool: Database connection pool.
        master_product_id (int): ID of the master product.
    """
    with get_db_connection(connection_pool) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM product_nutrition WHERE master_product_id = %s", (master_product_id,))
            conn.commit()
            logging.info(f"[clear_nutrition_for_product] Cleared existing nutrition for master_product_id {master_product_id}.")
        except Exception as e:
            conn.rollback()
            logging.error(f"[clear_nutrition_for_product ERROR] Failed to clear nutrition for master_product_id {master_product_id}: {e}")

# ------------------------------------------------------------------
# 10) Upsert Product & Alias
# ------------------------------------------------------------------
def upsert_product_and_alias(connection_pool: pool.ThreadedConnectionPool, product_json: Dict) -> Tuple[Optional[int], Optional[int]]:
    """
    Inserts or updates a master product and its alias, returning their IDs.

    Args:
        connection_pool: Database connection pool.
        product_json (dict): Detailed product information.

    Returns:
        tuple: (master_product_id, product_alias_id)
    """
    if not isinstance(product_json, dict):
        logging.error("[upsert_product_and_alias ERROR] product_json is not a dictionary.")
        return None, None

    sku = product_json.get("sku")  # e.g., "9414342140101"
    brand = product_json.get("brand", "")
    p_name = product_json.get("name", "")
    display_name = product_json.get("displayName", "")
    package_size = product_json.get("netContentUOM") or display_name

    with get_db_connection(connection_pool) as conn:
        try:
            with conn.cursor() as cur:
                # Upsert master_products
                if sku:
                    cur.execute("""
                    INSERT INTO master_products (barcode, brand, product_name, package_size)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (barcode)
                    DO UPDATE SET
                      brand = EXCLUDED.brand,
                      product_name = EXCLUDED.product_name,
                      package_size = EXCLUDED.package_size,
                      updated_at = NOW()
                    RETURNING id;
                    """, (sku, brand, p_name, package_size))
                else:
                    cur.execute("""
                    INSERT INTO master_products (barcode, brand, product_name, package_size)
                    VALUES (NULL, %s, %s, %s)
                    RETURNING id;
                    """, (brand, p_name, package_size))
                master_product_id = cur.fetchone()[0]
                logging.info(f"[upsert_product_and_alias] Upserted master_product_id {master_product_id}.")

                # Upsert product_aliases
                retailer_sku = product_json.get("productId")
                extra_json = {
                    "facets": product_json.get("facets", []),
                    "categories": product_json.get("categories", [])
                }

                product_alias_id = None
                if retailer_sku:
                    cur.execute("""
                    INSERT INTO product_aliases (
                      master_product_id,
                      retailer_name,
                      retailer_sku,
                      display_name,
                      extra_json
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (retailer_name, retailer_sku)
                    DO UPDATE SET
                      master_product_id = EXCLUDED.master_product_id,
                      display_name = EXCLUDED.display_name,
                      extra_json = EXCLUDED.extra_json,
                      updated_at = NOW()
                    RETURNING id;
                    """,
                    (
                        master_product_id,
                        CHAIN_NAME,
                        retailer_sku,
                        display_name,
                        json.dumps(extra_json)
                    ))
                    result = cur.fetchone()
                    if result:
                        product_alias_id = result[0]
                        logging.info(f"[upsert_product_and_alias] Upserted product_alias_id {product_alias_id} for product ID {retailer_sku}.")

                conn.commit()
                return master_product_id, product_alias_id
        except Exception as e:
            conn.rollback()
            logging.error(f"[upsert_product_and_alias ERROR] Failed to upsert product and/or alias: {e}")
            return None, None

# ------------------------------------------------------------------
# 11) Insert Price Snapshot (with cost-per-unit)
# ------------------------------------------------------------------
def insert_price_snapshot(connection_pool: pool.ThreadedConnectionPool, master_product_id: int, store_id: int, product_json: Dict) -> None:
    """
    Inserts a price snapshot for a product at a specific store.

    Args:
        connection_pool: Database connection pool.
        master_product_id (int): ID of the master product.
        store_id (int): ID of the store.
        product_json (dict): Detailed product information.
    """
    if not isinstance(product_json, dict):
        logging.error(f"[insert_price_snapshot ERROR] Expected dict for product_json, got {type(product_json)} for master_product_id {master_product_id}")
        return

    price_cents = product_json.get("price", 0)
    non_loyalty_cents = product_json.get("nonLoyaltyCardPrice", 0)
    price = price_cents / 100.0
    non_loyalty = non_loyalty_cents / 100.0 if non_loyalty_cents else None

    # Parse promotions
    promo_list = product_json.get("promotions", [])
    promo_price = None
    all_promos = []
    if promo_list and isinstance(promo_list, list):
        for p in promo_list:
            if not isinstance(p, dict):
                logging.debug(f"[insert_price_snapshot DEBUG] Skipping non-dict promotion: {p}")
                continue
            reward_val = p.get("rewardValue")
            if reward_val is not None:
                candidate = reward_val / 100.0
                if promo_price is None or candidate < promo_price:
                    promo_price = candidate
            all_promos.append(p)

    # Cost-per-unit
    comp_info = product_json.get("comparativePrice", {})
    if not isinstance(comp_info, dict):
        comp_info = product_json.get("singlePrice", {}).get("comparativePrice", {})

    unit_price = None
    unit_quantity = None
    unit_measure = None
    if isinstance(comp_info, dict):
        ppu = comp_info.get("pricePerUnit")  # e.g., 130 => $1.30
        if ppu is not None:
            unit_price = ppu / 100.0
        unit_quantity = comp_info.get("unitQuantity", None)
        unit_measure = comp_info.get("unitQuantityUom", None)

    with get_db_connection(connection_pool) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO prices (
                  master_product_id,
                  store_id,
                  price,
                  non_loyalty_price,
                  promo_price,
                  promo_data,
                  unit_price,
                  unit_quantity,
                  unit_measure,
                  scraped_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    master_product_id,
                    store_id,
                    price,
                    non_loyalty,
                    promo_price,
                    json.dumps(all_promos),
                    unit_price,
                    unit_quantity,
                    unit_measure,
                    datetime.now(timezone.utc)
                ))
            conn.commit()
            logging.info(f"[insert_price_snapshot] Inserted price snapshot for master_product_id {master_product_id} at store_id {store_id}.")
        except Exception as e:
            conn.rollback()
            logging.error(f"[insert_price_snapshot ERROR] Failed to insert price snapshot for master_product_id {master_product_id}: {e}")
            raise

def has_price_snapshot_today(connection_pool: pool.ThreadedConnectionPool, master_product_id: int, store_id: int) -> bool:
    """
    Checks if a price snapshot exists for the given product and store for today (UTC).

    Args:
        connection_pool: Database connection pool.
        master_product_id (int): ID of the master product.
        store_id (int): ID of the store.

    Returns:
        bool: True if a snapshot exists, False otherwise.
    """
    # Get the current UTC date range
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    with get_db_connection(connection_pool) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM prices
                    WHERE master_product_id = %s
                      AND store_id = %s
                      AND scraped_at >= %s
                      AND scraped_at < %s
                    LIMIT 1;
                """, (master_product_id, store_id, today_start, today_end))
                exists = cur.fetchone() is not None
                logging.debug(f"[has_price_snapshot_today] Snapshot exists: {exists} for master_product_id {master_product_id} at store_id {store_id}.")
                return exists
        except Exception as e:
            logging.error(f"[has_price_snapshot_today ERROR] Failed to check price snapshot for master_product_id {master_product_id} at store_id {store_id}: {e}")
            return False

# ------------------------------------------------------------------
# 12) Linking Product to Leaf Category
# ------------------------------------------------------------------
def link_categories(connection_pool: pool.ThreadedConnectionPool, product_alias_id: int, category_path: List[str]) -> None:
    """
    Links a product alias to its leaf category.

    Args:
        connection_pool: Database connection pool.
        product_alias_id (int): ID of the product alias.
        category_path (list): List of category names representing the path.
    """
    if not category_path:
        return
    leaf_cat_id = upsert_category(connection_pool, category_path)
    if leaf_cat_id:
        link_product_to_category(connection_pool, product_alias_id, leaf_cat_id)
        logging.info(f"[link_categories] Linked category ID {leaf_cat_id} for product_alias_id {product_alias_id}.")

# ------------------------------------------------------------------
# 13) Store Nutrition
# ------------------------------------------------------------------
def store_nutrition(connection_pool: pool.ThreadedConnectionPool, master_product_id: int, product_json: Dict) -> None:
    """
    Stores nutritional information for a product.

    Args:
        connection_pool: Database connection pool.
        master_product_id (int): ID of the master product.
        product_json (dict): Detailed product information.
    """
    nut_info = product_json.get("nutritionalInfo", {})
    nutrients = nut_info.get("nutrients", [])

    clear_nutrition_for_product(connection_pool, master_product_id)
    for n_obj in nutrients:
        if not isinstance(n_obj, dict):
            logging.debug(f"[store_nutrition DEBUG] Skipping non-dict nutrient: {n_obj}")
            continue
        insert_nutrition(connection_pool, master_product_id, n_obj)

# ------------------------------------------------------------------
# Connection Context Manager
# ------------------------------------------------------------------
@contextmanager
def get_db_connection(connection_pool: pool.ThreadedConnectionPool):
    """
    Context manager to acquire and release a database connection.

    Args:
        connection_pool: Database connection pool.

    Yields:
        connection: A psycopg2 database connection.
    """
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main():
    """
    The main function orchestrates the scraping process:
    1. Initializes logging and database connections.
    2. Authenticates and retrieves the list of stores.
    3. Iterates through each store and its categories to scrape products.
    """
    setup_logging()  # Initialize logging with both file and console handlers
    logging.info("[main] Starting scraper...")

    # 1) Initialize the database connection pool
    try:
        connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=30,  # Increased from 20 to 30 to accommodate more concurrent threads
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        if connection_pool:
            logging.info("[main] Database connection pool created successfully.")
    except Exception as e:
        logging.critical(f"[main ERROR] Failed to create database connection pool: {e}")
        return

    # 2) Initialize the database tables
    try:
        logging.info("[main] Initializing the database...")
        init_db(connection_pool)
    except Exception as e:
        logging.critical(f"[main ERROR] Database initialization failed: {e}")
        connection_pool.closeall()
        return

    # 3) Create a shared session and update with initial cookies
    session = requests.Session()
    session.cookies.update(INITIAL_COOKIES)
    logging.info("[main] HTTP session initialized with cookies.")

    # Initialize TokenManager
    token_manager = TokenManager(session)

    # 4) Get authentication token
    try:
        logging.info("[main] Fetching authentication token...")
        token_manager.refresh_token()
        if token_manager.token:
            logging.info(f"[main] Authentication token retrieved: {token_manager.token[:30]}...")
        else:
            logging.error("[main] No token retrieved. Exiting.")
            connection_pool.closeall()
            return
    except Exception as e:
        logging.critical(f"[main ERROR] Authentication failed: {e}")
        connection_pool.closeall()
        return

    # 5) Get all New World stores
    try:
        logging.info("[main] Fetching store list...")
        store_list = get_stores(session, token_manager.token)
    except Exception as e:
        logging.critical(f"[main ERROR] Failed to retrieve stores: {e}")
        connection_pool.closeall()
        return

    # 6) Scrape products from stores and subcategories
    limited_store_list = store_list[:MAX_STORES]
    total_stores = len(limited_store_list)
    for s_idx, s_obj in enumerate(limited_store_list, start=1):
        try:
            store_db_id = upsert_store(connection_pool, s_obj)
        except Exception as e:
            logging.error(f"[main ERROR] Failed to upsert store '{s_obj.get('name', 'Unknown')}': {e}")
            continue

        store_name = s_obj.get("name", "Unknown")
        ecom_store_id = s_obj.get("EcomStoreId")
        logging.info(f"\n--- Scraping store '{store_name}' (Store ID: {ecom_store_id}) [{s_idx}/{total_stores}] ---")

        # Fetch categories for the current store
        categories_url = f"{API_BASE_URL}/v1/edge/store/{ecom_store_id}/categories"
        headers = dict(HEADERS)
        if token_manager.token:
            headers["Authorization"] = f"Bearer {token_manager.get_valid_token()}"

        try:
            logging.info(f"[main] Fetching categories from {categories_url}")
            resp = session.get(categories_url, headers=headers, timeout=10)  # Added timeout
            logging.info(f"[main] Categories response status: {resp.status_code}")
            if resp.status_code == 401:
                logging.warning(f"[main] Unauthorized access detected while fetching categories. Refreshing token and retrying...")
                token_manager.refresh_token()
                headers["Authorization"] = f"Bearer {token_manager.get_valid_token()}"
                resp = session.get(categories_url, headers=headers, timeout=10)
                logging.info(f"[main] Retry categories response status: {resp.status_code}")
            resp.raise_for_status()
            category_data = resp.json()
            logging.info(f"[main] Fetched categories successfully for store '{store_name}'.")
        except requests.HTTPError as e:
            logging.error(f"[main ERROR] Failed to fetch categories for store '{store_name}': {e}")
            continue
        except Exception as e:
            logging.error(f"[main ERROR] Unexpected error while fetching categories for store '{store_name}': {e}")
            continue

        # Extract all leaf categories
        try:
            leaf_categories = extract_leaf_categories(category_data)
            logging.info(f"[main] Extracted {len(leaf_categories)} leaf categories for store '{store_name}'.")
        except Exception as e:
            logging.error(f"[main ERROR] Failed to extract leaf categories for store '{store_name}': {e}")
            continue

        if not leaf_categories:
            logging.info(f"[main] No leaf categories found for store '{store_name}'. Skipping.")
            continue

        # Define the number of worker threads for categories
        # Assuming each category will spawn up to 4 product threads
        # Total threads = category_max_workers * product_max_workers <= maxconn
        # Here, maxconn=30, category_max_workers=5, product_max_workers=4 => 20 concurrent DB operations
        category_max_workers = min(5, len(leaf_categories)) if leaf_categories else 1

        # Progress bar for categories
        if tqdm:
            categories_pbar = tqdm(total=len(leaf_categories), desc=f"Scraping Store: {store_name}", unit="category")
        else:
            categories_pbar = None

        with ThreadPoolExecutor(max_workers=category_max_workers) as category_executor:
            category_futures = []
            for category_path in leaf_categories:
                category_futures.append(
                    category_executor.submit(
                        process_category,
                        connection_pool,
                        token_manager,
                        ecom_store_id,
                        store_db_id,
                        category_path
                    )
                )

            for future in as_completed(category_futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"[main ERROR] Error in processing category: {e}")
                finally:
                    if categories_pbar:
                        categories_pbar.update(1)

        if categories_pbar:
            categories_pbar.close()

    # Close all connections in the pool
    connection_pool.closeall()
    logging.info("[main] Scraper finished successfully.")

def process_category(connection_pool: pool.ThreadedConnectionPool, token_manager: 'TokenManager', ecom_store_id: str, store_db_id: int, category_path: List[str]) -> None:
    """
    Processes a single category: fetches products and handles each product.

    Args:
        connection_pool: Database connection pool.
        token_manager (TokenManager): Instance managing the authentication token.
        ecom_store_id (str): Store ID.
        store_db_id (int): Database store ID.
        category_path (list): List representing the category hierarchy.
    """
    category_name = " > ".join(category_path)
    logging.info(f"\n  [Category] Scraping category: '{category_name}'")

    # Each thread uses the shared session from the token manager
    session = token_manager.session

    # Fetch all product summaries for the store and subcategory
    try:
        summaries = get_all_products_for_store(session, token_manager, ecom_store_id, category_path=category_path)
        logging.info(f"    [Category] Found {len(summaries)} product summaries in category '{category_name}'.")
    except Exception as e:
        logging.error(f"    [Category ERROR] Failed to get product summaries for category '{category_name}': {e}")
        return

    if not summaries:
        return

    # Define the number of worker threads for products
    product_max_workers = min(4, len(summaries)) if summaries else 1

    # Progress bar for products
    if tqdm:
        products_pbar = tqdm(total=len(summaries), desc=f"    Processing Category: {category_name}", unit="product")
    else:
        products_pbar = None

    with ThreadPoolExecutor(max_workers=product_max_workers) as product_executor:
        product_futures = []
        for psum in summaries:
            product_id = psum.get("productId")
            if not product_id:
                logging.warning(f"      [Product] Skipping product with missing productId.")
                if products_pbar:
                    products_pbar.update(1)
                continue
            product_futures.append(
                product_executor.submit(
                    process_product,
                    connection_pool,
                    token_manager,
                    ecom_store_id,
                    store_db_id,
                    product_id,
                    category_path
                )
            )

        for future in as_completed(product_futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"      [Product ERROR] Error in processing product: {e}")
            finally:
                if products_pbar:
                    products_pbar.update(1)

    if products_pbar:
        products_pbar.close()

    # Optional: Sleep to respect API rate limits between categories
    time.sleep(0.2)

def process_product(connection_pool: pool.ThreadedConnectionPool, token_manager: 'TokenManager', ecom_store_id: str, store_db_id: int, product_id: str, category_path: List[str]) -> None:
    """
    Processes a single product: fetches details, upserts into DB, links categories, stores nutrition, and inserts price snapshot.

    Args:
        connection_pool: Database connection pool.
        token_manager (TokenManager): Instance managing the authentication token.
        ecom_store_id (str): Store ID.
        store_db_id (int): Database store ID.
        product_id (str): Product ID.
        category_path (list): List representing the category hierarchy.
    """
    session = token_manager.session

    # Check if a price snapshot already exists for today
    try:
        # To implement this, you need to upsert the product first to get master_product_id
        # Then check if a snapshot exists
        # Placeholder for future implementation
        pass
    except Exception as e:
        logging.error(f"        [Product ERROR] Failed to check price snapshot for product ID {product_id}: {e}")
        return

    # Fetch detailed product information
    try:
        detail = get_product_detail(session, token_manager, ecom_store_id, product_id)
        if not detail:
            logging.warning(f"        [Product] No detail data for product ID {product_id}. Skipping.")
            return
    except Exception as e:
        logging.error(f"        [Product ERROR] Failed to fetch details for product ID {product_id}: {e}")
        return

    # Upsert product and alias, then handle categories, nutrition, and pricing
    try:
        mp_id, pa_id = upsert_product_and_alias(connection_pool, detail)
        if mp_id is None or pa_id is None:
            logging.error(f"          [DB ERROR] Failed to upsert product and/or alias for product ID {product_id}.")
            return

        # Link categories
        if pa_id:
            link_categories(connection_pool, pa_id, category_path)

        # Store nutrition information
        store_nutrition(connection_pool, mp_id, detail)

        # Insert price snapshot
        insert_price_snapshot(connection_pool, mp_id, store_db_id, detail)

    except Exception as e:
        logging.error(f"        [Product ERROR] Database error for product ID {product_id}: {e}")
        return

    # Optional: Sleep to respect API rate limits
    time.sleep(0.05)

# ------------------------------------------------------------------
# Additional Utilities and Functions (e.g., Monitoring)
# ------------------------------------------------------------------
# You can add additional functions here for monitoring connection pool usage
# or any other utilities as needed.

if __name__ == "__main__":
    main()
