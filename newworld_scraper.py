#!/usr/bin/env python3

import os
import json
import time
import datetime
import requests
import psycopg2
from psycopg2.extras import execute_values
import logging

# ------------------------------------------------------------------
# Configure Logging
# ------------------------------------------------------------------
logging.basicConfig(
    filename='scraper.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # Set to INFO or WARNING in production
)

# ------------------------------------------------------------------
# 1) Load Config
# ------------------------------------------------------------------
def load_config():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, "config.json")
    if not os.path.exists(config_path):
        logging.critical("config.json not found.")
        raise FileNotFoundError("config.json not found.")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

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
def init_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()

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
          updated_at TIMESTAMP
        );
        """)
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'product_aliases_retailer_name_sku_unique'
            ) THEN
                ALTER TABLE product_aliases
                ADD CONSTRAINT product_aliases_retailer_name_sku_unique
                UNIQUE (retailer_name, retailer_sku);
                RAISE NOTICE 'Unique constraint product_aliases_retailer_name_sku_unique added.';
            ELSE
                RAISE NOTICE 'Unique constraint product_aliases_retailer_name_sku_unique already exists.';
            END IF;
        END$$;
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
        cur.close()
        conn.close()
        logging.info("[init_db] Database initialized successfully.")
    except Exception as e:
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
# 4) Authentication Handling
# ------------------------------------------------------------------
def get_auth_token(session: requests.Session) -> str:
    """
    Attempt to replicate the successful GET /CommonApi/Account/GetCurrentUser
    with the same cookies/headers. If it returns 200, parse the JSON.
    If the site still 403s, you may need additional cookies or a real login flow.
    """
    url = f"{NEW_WORLD_BASE_URL}/CommonApi/Account/GetCurrentUser"
    try:
        logging.info(f"[get_auth_token] Making request to {url}")
        resp = session.get(url, headers=HEADERS, timeout=10)
        logging.info(f"[get_auth_token] Response status: {resp.status_code}")
        resp.raise_for_status()  # will raise HTTPError if 403 or 401
        data = resp.json()
        logging.debug(f"[get_auth_token DEBUG] Response Data: {json.dumps(data, indent=2)}")
        token = data.get("access_token")
        if not token:
            logging.error("[get_auth_token] No access_token found in response. Are you sure the user is logged in?")
            return ""
        logging.info("[get_auth_token] Authentication token retrieved successfully.")
        logging.debug(f"[get_auth_token DEBUG] Token: {token}")
        return token
    except requests.HTTPError as e:
        logging.error(f"[get_auth_token ERROR] Failed to get auth token: {e}")
        raise
    except Exception as e:
        logging.error(f"[get_auth_token ERROR] Unexpected error while getting auth token: {e}")
        raise

# ------------------------------------------------------------------
# 5) Store Upsert
# ------------------------------------------------------------------
def get_stores(session: requests.Session, token: str) -> list:
    """
    Fetch the list of stores from the API.
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

def upsert_store(conn, store_obj: dict) -> int:
    """
    Insert or update a store in the database and return its ID.
    """
    ecom_id = store_obj.get("EcomStoreId")
    name = store_obj.get("name", "")
    logging.debug(f"[upsert_store DEBUG] Processing store '{name}' with EcomStoreId: {ecom_id}")
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
        return store_id

# ------------------------------------------------------------------
# 6) Paginated Products
# ------------------------------------------------------------------
def get_paginated_products(session: requests.Session, token: str, store_id: str, category_path: list, page: int=0, hits_per_page: int=50) -> dict:
    """
    Get paginated products from the API using page and hitsPerPage for pagination, filtered by category hierarchy.
    
    Args:
        session (requests.Session): The session object.
        token (str): Authentication token.
        store_id (str): Store ID.
        category_path (list): List of category names representing the path, e.g., ["Kitchen, Dining & Household", "Laundry", "Laundry Powders"]
        page (int): Page number.
        hits_per_page (int): Number of items per page.
    
    Returns:
        dict: API response.
    """
    url = f"{API_BASE_URL}/v1/edge/search/paginated/products"

    headers = dict(HEADERS)
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

        if resp.status_code != 200:
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

    except requests.exceptions.RequestException as e:
        logging.error(f"[get_paginated_products ERROR] Request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"[get_paginated_products ERROR] Response content: {e.response.text}")
        else:
            logging.error("[get_paginated_products ERROR] No response content available.")
        raise

def get_all_products_for_store(session: requests.Session, token: str, store_id: str, category_path: list) -> list:
    """
    Loops through the pages of products for a given store and category using page-based pagination.
    
    Args:
        session (requests.Session): The session object.
        token (str): Authentication token.
        store_id (str): Store ID.
        category_path (list): List of category names representing the path.
    
    Returns:
        list: List of product summaries.
    """
    all_items = []
    page = 0
    hits_per_page = 50  # Number of items per request
    while True:
        try:
            data = get_paginated_products(session, token, store_id, category_path=category_path, page=page, hits_per_page=hits_per_page)
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
def get_product_detail(session: requests.Session, token: str, store_id: str, product_id: str) -> dict:
    """
    Fetch detailed information for a specific product.
    """
    url = f"{API_BASE_URL}/v1/edge/store/{store_id}/product/{product_id}"
    headers = dict(HEADERS)
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        logging.info(f"[get_product_detail] Making request to {url}")
        resp = session.get(url, headers=headers, timeout=10)
        logging.info(f"[get_product_detail] Response status: {resp.status_code}")
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
def extract_leaf_categories(categories, parent_path=None):
    """
    Recursively extract all leaf subcategories from the category tree.
    
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

def upsert_category(conn, category_path: list) -> int:
    """
    Insert or update a category hierarchy and return the leaf category ID.
    
    Args:
        conn: Database connection.
        category_path (list): List of category names representing the path.
    
    Returns:
        int: ID of the leaf category.
    """
    parent_id = None
    for cat_name in category_path:
        cat_name = cat_name.strip()
        if not cat_name:
            continue
        with conn.cursor() as cur:
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
    return parent_id

def link_product_to_category(conn, product_alias_id: int, category_id: int):
    """
    Link a product alias to a category.
    """
    if not category_id:
        return
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO product_categories (product_alias_id, category_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """, (product_alias_id, category_id))
    logging.info(f"[link_product_to_category] Linked product_alias_id {product_alias_id} to category_id {category_id}.")

# ------------------------------------------------------------------
# 9) Nutrition
# ------------------------------------------------------------------
def insert_nutrition(conn, master_product_id: int, nut_data: dict):
    """
    Insert nutritional information for a product.
    """
    code = nut_data.get("nutrientType", "")
    name = nut_data.get("nutrientTypeDescription", "")
    amount = nut_data.get("qtyContained", 0)
    uom = nut_data.get("nutrientUom", "")
    basis = nut_data.get("nutrientBasisQty", 100)
    precision = nut_data.get("measurementPrecision", "")

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
    logging.info(f"[insert_nutrition] Inserted nutrition for master_product_id {master_product_id}.")

def clear_nutrition_for_product(conn, master_product_id: int):
    """
    Clear existing nutritional information for a product.
    """
    with conn.cursor() as cur:
        cur.execute("DELETE FROM product_nutrition WHERE master_product_id = %s", (master_product_id,))
    logging.info(f"[clear_nutrition_for_product] Cleared existing nutrition for master_product_id {master_product_id}.")

# ------------------------------------------------------------------
# 10) Upsert Product & Alias
# ------------------------------------------------------------------
def upsert_product_and_alias(conn, product_json: dict) -> (int, int): # type: ignore
    """
    Insert or update a master product and its alias, returning their IDs.
    
    Args:
        conn: Database connection.
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

        return master_product_id, product_alias_id

# ------------------------------------------------------------------
# 11) Insert Price Snapshot (with cost-per-unit)
# ------------------------------------------------------------------
def insert_price_snapshot(conn, master_product_id: int, store_id: int, product_json: dict):
    """
    Insert a price snapshot for a product at a specific store.
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

    with conn.cursor() as cur:
        try:
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
                datetime.datetime.now(datetime.timezone.utc)
            ))
            logging.info(f"[insert_price_snapshot] Inserted price snapshot for master_product_id {master_product_id} at store_id {store_id}.")
        except Exception as e:
            logging.error(f"[insert_price_snapshot ERROR] Failed to insert price snapshot for master_product_id {master_product_id}: {e}")
            raise

def has_price_snapshot_today(conn, master_product_id: int, store_id: int) -> bool:
    """
    Check if a price snapshot exists for the given product and store for today (UTC).
    
    Args:
        conn: Database connection.
        master_product_id (int): ID of the master product.
        store_id (int): ID of the store.
    
    Returns:
        bool: True if a snapshot exists, False otherwise.
    """
    # Get the current UTC date range
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM prices
            WHERE master_product_id = %s
              AND store_id = %s
              AND scraped_at >= %s
              AND scraped_at < %s
            LIMIT 1;
        """, (master_product_id, store_id, today_start, today_end))
        return cur.fetchone() is not None

# ------------------------------------------------------------------
# 12) Linking Product to Leaf Category
# ------------------------------------------------------------------
def link_categories(conn, product_alias_id: int, category_path: list):
    """
    Link product alias to its leaf category.
    """
    if not category_path:
        return
    leaf_cat_id = upsert_category(conn, category_path)
    if leaf_cat_id:
        link_product_to_category(conn, product_alias_id, leaf_cat_id)
        logging.info(f"[link_categories] Linked category ID {leaf_cat_id} for product_alias_id {product_alias_id}.")

# ------------------------------------------------------------------
# 13) Store Nutrition
# ------------------------------------------------------------------
def store_nutrition(conn, master_product_id: int, product_json: dict):
    """
    Store nutritional information for a product.
    """
    nut_info = product_json.get("nutritionalInfo", {})
    nutrients = nut_info.get("nutrients", [])

    clear_nutrition_for_product(conn, master_product_id)
    for n_obj in nutrients:
        if not isinstance(n_obj, dict):
            logging.debug(f"[store_nutrition DEBUG] Skipping non-dict nutrient: {n_obj}")
            continue
        insert_nutrition(conn, master_product_id, n_obj)

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main():
    logging.info("[main] Starting scraper...")

    # 1) Initialize the database
    try:
        logging.info("[main] Initializing the database...")
        init_db()
    except Exception as e:
        logging.critical(f"[main ERROR] Database initialization failed: {e}")
        return

    # 2) Create a session and update with initial cookies
    session = requests.Session()
    session.cookies.update(INITIAL_COOKIES)
    logging.info("[main] HTTP session initialized with cookies.")

    # 3) Get authentication token
    try:
        logging.info("[main] Fetching authentication token...")
        token = get_auth_token(session)
        if token:
            logging.info(f"[main] Authentication token retrieved: {token[:30]}...")
        else:
            logging.error("[main] No token retrieved. Exiting.")
            return
    except Exception as e:
        logging.critical(f"[main ERROR] Authentication failed: {e}")
        return

    # 4) Get all New World stores
    try:
        logging.info("[main] Fetching store list...")
        store_list = get_stores(session, token)
    except Exception as e:
        logging.critical(f"[main ERROR] Failed to retrieve stores: {e}")
        return

    # 5) Connect to the database
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        conn.autocommit = False
        logging.info("[main] Connected to the database.")
    except Exception as e:
        logging.critical(f"[main ERROR] Failed to connect to the database: {e}")
        return

    # 6) Scrape products from stores and subcategories
    limited_store_list = store_list[:MAX_STORES]
    for s_idx, s_obj in enumerate(limited_store_list, start=1):
        try:
            store_db_id = upsert_store(conn, s_obj)
            conn.commit()
        except Exception as e:
            logging.error(f"[main ERROR] Failed to upsert store '{s_obj.get('name', 'Unknown')}': {e}")
            conn.rollback()
            continue

        store_name = s_obj.get("name", "Unknown")
        ecom_store_id = s_obj.get("EcomStoreId")
        logging.info(f"\n--- Scraping store '{store_name}' (Store ID: {ecom_store_id}) [{s_idx}/{MAX_STORES}] ---")

        # Fetch categories for the current store
        categories_url = f"{API_BASE_URL}/v1/edge/store/{ecom_store_id}/categories"
        headers = dict(HEADERS)
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            logging.info(f"[main] Fetching categories from {categories_url}")
            resp = session.get(categories_url, headers=headers, timeout=10)  # Added timeout
            logging.info(f"[main] Categories response status: {resp.status_code}")
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

        # Iterate through each leaf subcategory
        for c_idx, category_path in enumerate(leaf_categories, start=1):
            category_name = " > ".join(category_path)  # e.g., "Fresh Foods & Bakery > Butchery > Fresh Beef & Lamb"
            logging.info(f"\n  [Category {c_idx}/{len(leaf_categories)}] Scraping category: '{category_name}'")

            # Fetch all product summaries for the store and subcategory
            try:
                summaries = get_all_products_for_store(session, token, ecom_store_id, category_path=category_path)
                logging.info(f"    [main] Found {len(summaries)} product summaries in category '{category_name}'.")
            except Exception as e:
                logging.error(f"    [main ERROR] Failed to get product summaries for category '{category_name}': {e}")
                continue

            # Iterate through each product summary and process details
            for idx, psum in enumerate(summaries):
                product_id = psum.get("productId")
                if not product_id:
                    logging.warning(f"      [Product {idx + 1}/{len(summaries)}] Skipping product with missing productId.")
                    continue

                # Fetch detailed product information
                try:
                    detail = get_product_detail(session, token, ecom_store_id, product_id)
                    if not detail:
                        logging.warning(f"      [Product {idx + 1}/{len(summaries)}] No detail data for product ID {product_id}. Skipping.")
                        continue
                except Exception as e:
                    logging.error(f"      [Product {idx + 1}/{len(summaries)} ERROR] Failed to fetch details for product ID {product_id}: {e}")
                    continue

                # Upsert product and alias, then handle categories, nutrition, and pricing
                try:
                    mp_id, pa_id = upsert_product_and_alias(conn, detail)
                    if mp_id is None or pa_id is None:
                        logging.error(f"        [DB ERROR] Failed to upsert product and/or alias for product ID {product_id}. Rolling back.")
                        conn.rollback()
                        continue

                    # Link categories
                    if pa_id:
                        link_categories(conn, pa_id, category_path)

                    # Store nutrition information
                    store_nutrition(conn, mp_id, detail)

                    # Insert price snapshot
                    insert_price_snapshot(conn, mp_id, store_db_id, detail)

                    # Commit the transaction
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logging.error(f"        [DB ERROR] Database error for product ID {product_id}: {e}")
                    continue

                # Optional: Sleep to respect API rate limits
                time.sleep(0.05)  # Adjust as needed

if __name__ == "__main__":
    main()
