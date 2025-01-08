#!/usr/bin/env python3

import os
import json
import time
import datetime
import requests
import psycopg2
from psycopg2.extras import execute_values

# ------------------------------------------------------------------
# 1) Load Config
# ------------------------------------------------------------------
def load_config():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, "config.json")
    if not os.path.exists(config_path):
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

# ------------------------------------------------------------------
# 2) Initialize DB / Full Relational Schema
# ------------------------------------------------------------------
def init_db():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
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

    # prices
    cur.execute("""
    CREATE TABLE IF NOT EXISTS prices (
      id SERIAL PRIMARY KEY,
      master_product_id INT NOT NULL REFERENCES master_products(id),
      store_id INT NOT NULL REFERENCES stores(id),
      price NUMERIC(10,2) NOT NULL,
      non_loyalty_price NUMERIC(10,2),
      promo_price NUMERIC(10,2),
      promo_data JSONB,
      scraped_at TIMESTAMP NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)

    # categories
    cur.execute("""
    CREATE TABLE IF NOT EXISTS categories (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      parent_id INT REFERENCES categories(id),
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMP
    );
    """)

    # bridging: product_categories
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
      nutrient_code TEXT,            -- e.g. "FAT", "PRO-", "SUGAR", etc.
      nutrient_name TEXT NOT NULL,   -- "Fat - Total", "Protein", etc.
      amount NUMERIC(10,2),
      unit TEXT,                     -- "GRM", "KJO", etc.
      basis_qty NUMERIC(10,2),       -- usually 100 for "per 100g"
      measurement_precision TEXT,    -- EXACT, LESS_THAN
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMP
    );
    """)

    # Make prices a Timescale hypertable
    try:
        cur.execute("""
            CREATE EXTENSION IF NOT EXISTS timescaledb;
            SELECT create_hypertable('prices', 'scraped_at', if_not_exists => TRUE);
        """)
    except Exception as e:
        print("Timescale hypertable error:", e)

    conn.commit()
    cur.close()
    conn.close()

# ------------------------------------------------------------------
# 3) Auth
# ------------------------------------------------------------------
def get_auth_token(session: requests.Session) -> str:
    url = f"{NEW_WORLD_BASE_URL}/CommonApi/Account/GetCurrentUser"
    resp = session.get(url)
    resp.raise_for_status()
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("No access_token found.")
    return token

# ------------------------------------------------------------------
# 4) Get Stores
# ------------------------------------------------------------------
def get_stores(session: requests.Session, token: str) -> list:
    url = f"{NEW_WORLD_BASE_URL}/BrandsApi/BrandsStore/GetBrandStores"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    resp = session.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()  # list of store objects

def upsert_store(conn, store_obj: dict) -> int:
    ecom_id = store_obj.get("EcomStoreId")
    name = store_obj.get("name", "")
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO stores (retailer_name, store_name, retailer_store_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (retailer_store_id)
        DO UPDATE SET store_name = EXCLUDED.store_name, updated_at = NOW()
        RETURNING id;
        """, (CHAIN_NAME, name, ecom_id))
        return cur.fetchone()[0]

# ------------------------------------------------------------------
# 5) Paginated Product Summaries
# ------------------------------------------------------------------
def get_paginated_products(session: requests.Session, token: str, store_id: str, page: int, hits_per_page: int) -> dict:
    url = f"{API_BASE_URL}/v1/edge/search/paginated/products"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "page": page,
        "hitsPerPage": hits_per_page
    }
    resp = session.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()

def get_all_products_for_store(session: requests.Session, token: str, ecom_store_id: str) -> list:
    all_items = []
    page = 0
    hits_per_page = 50
    while True:
        data = get_paginated_products(session, token, ecom_store_id, page, hits_per_page)
        products = data.get("products", [])
        if not products:
            break
        all_items.extend(products)

        cur_page = data.get("page", page)
        total_pages = data.get("totalPages", 1)
        print(f"  [get_all_products_for_store] page {cur_page+1}/{total_pages}, got {len(products)} items")

        if cur_page >= (total_pages - 1):
            break
        page += 1
        time.sleep(0.25)

    return all_items

# ------------------------------------------------------------------
# 6) Detailed Product
# ------------------------------------------------------------------
def get_product_detail(session: requests.Session, token: str, store_id: str, product_id: str) -> dict:
    url = f"{API_BASE_URL}/v1/edge/store/{store_id}/product/{product_id}"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    resp = session.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

# ------------------------------------------------------------------
# 7) Categories (Relational)
# ------------------------------------------------------------------
def upsert_category(conn, category_name: str) -> int:
    """
    Insert or find a category row by name.
    Returns category_id.
    """
    category_name = category_name.strip()
    if not category_name:
        return None
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO categories (name)
        VALUES (%s)
        ON CONFLICT (name)
        DO UPDATE SET updated_at = NOW()
        RETURNING id;
        """, (category_name,))
        row = cur.fetchone()
        return row[0]

def link_product_to_category(conn, product_alias_id: int, category_id: int):
    """
    Insert a row in product_categories if not exists.
    """
    if not category_id:
        return
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO product_categories (product_alias_id, category_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """, (product_alias_id, category_id))

# ------------------------------------------------------------------
# 8) Nutrition (Relational)
# ------------------------------------------------------------------
def insert_nutrition(conn, master_product_id: int, nut_data: dict):
    """
    Suppose nut_data is each "nutrients" array item:
      {
        "nutrientType": "FAT",
        "nutrientTypeDescription": "Fat - Total",
        "qtyContained": 81.4,
        "nutrientUom": "GRM",
        "nutrientBasisQty": 100,
        "measurementPrecision": "EXACT",
        ...
      }
    We'll insert a row in product_nutrition.
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

def clear_nutrition_for_product(conn, master_product_id: int):
    """
    If you want to remove old rows before re-inserting fresh data each scrape,
    you can do so here. Or we can just append. 
    For a time-series approach, you might store multiple versions.
    For simplicity, let's just wipe old rows each time we get new data.
    """
    with conn.cursor() as cur:
        cur.execute("DELETE FROM product_nutrition WHERE master_product_id = %s", (master_product_id,))

# ------------------------------------------------------------------
# 9) Upsert Product & Alias
# ------------------------------------------------------------------
def upsert_product_and_alias(conn, product_json: dict) -> (int, int):
    """
    Upserts into master_products (by 'sku' -> barcode) and product_aliases (by retailer_sku).
    Returns (master_product_id, product_alias_id).
    """
    sku = product_json.get("sku")  # e.g. "9414342140101"
    brand = product_json.get("brand", "")
    p_name = product_json.get("name", "")
    display_name = product_json.get("displayName", "")
    package_size = product_json.get("netContentUOM") or display_name

    with conn.cursor() as cur:
        # master_products by barcode
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

        # product_aliases
        retailer_sku = product_json.get("productId")
        extra_json = {}
        # store some raw fields if wanted
        extra_json["facets"] = product_json.get("facets", [])
        extra_json["categories"] = product_json.get("categories", [])
        # etc.

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
            product_alias_id = cur.fetchone()[0]
        else:
            product_alias_id = None

        return master_product_id, product_alias_id

# ------------------------------------------------------------------
# 10) Insert Price Snapshot
# ------------------------------------------------------------------
def insert_price_snapshot(conn, master_product_id: int, store_id: int, product_json: dict):
    price_cents = product_json.get("price", 0)
    non_loyalty_cents = product_json.get("nonLoyaltyCardPrice", 0)
    price = price_cents / 100.0
    non_loyalty = non_loyalty_cents / 100.0 if non_loyalty_cents else None

    # promotions
    promo_list = product_json.get("promotions", [])
    promo_price = None
    all_promos = []
    if promo_list:
        for p in promo_list:
            reward_val = p.get("rewardValue")
            if reward_val is not None:
                candidate = reward_val / 100.0
                if promo_price is None or candidate < promo_price:
                    promo_price = candidate
            all_promos.append(p)

    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO prices (
          master_product_id,
          store_id,
          price,
          non_loyalty_price,
          promo_price,
          promo_data,
          scraped_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            master_product_id,
            store_id,
            price,
            non_loyalty,
            promo_price,
            json.dumps(all_promos),
            datetime.datetime.utcnow()
        ))

# ------------------------------------------------------------------
# 11) Link Categories
# ------------------------------------------------------------------
def link_categories(conn, product_alias_id: int, product_json: dict):
    """
    NW might have product_json["categories"] = ["Dairy & Eggs","Butter & Spreads"]
    or product_json["categoryTrees"] for hierarchical data.
    We'll just parse 'categories' for simplicity.
    """
    categories = product_json.get("categories", [])
    for cat_name in categories:
        cat_id = upsert_category(conn, cat_name)
        if cat_id:
            link_product_to_category(conn, product_alias_id, cat_id)

# ------------------------------------------------------------------
# 12) Store Nutrition
# ------------------------------------------------------------------
def store_nutrition(conn, master_product_id: int, product_json: dict):
    """
    NW's "nutritionalInfo" might look like:
    {
      "nutrients": [
         {
           "nutrientType": "FAT", ...
         }, ...
      ]
    }
    We'll parse them into product_nutrition.
    We'll remove old rows first (so it's always fresh).
    """
    nut_info = product_json.get("nutritionalInfo", {})
    nutrients = nut_info.get("nutrients", [])

    # remove old
    clear_nutrition_for_product(conn, master_product_id)

    for n_obj in nutrients:
        insert_nutrition(conn, master_product_id, n_obj)

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main():
    # init db
    init_db()

    # session
    session = requests.Session()

    # get token
    token = get_auth_token(session)
    print("[main] Got token:", token[:30], "...")

    # get stores
    store_list = get_stores(session, token)
    print(f"[main] Found {len(store_list)} stores from NW")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = False

    # For demonstration, limit to first 2
    stores_to_scrape = store_list[:2]

    for s_obj in stores_to_scrape:
        store_db_id = upsert_store(conn, s_obj)
        conn.commit()

        store_name = s_obj.get("name", "Unknown")
        ecom_store_id = s_obj.get("EcomStoreId")
        print(f"\n--- Scraping store '{store_name}' ({ecom_store_id}) ---")

        # product list
        summaries = get_all_products_for_store(session, token, ecom_store_id)
        print(f"  [main] found {len(summaries)} product summaries.")

        for idx, psum in enumerate(summaries):
            product_id = psum.get("productId")
            if not product_id:
                continue

            # fetch detail
            try:
                detail = get_product_detail(session, token, ecom_store_id, product_id)
            except requests.HTTPError as e:
                print("[ERROR] product detail fetch failed:", e)
                continue

            # upsert
            try:
                mp_id, pa_id = upsert_product_and_alias(conn, detail)
                # categories
                if pa_id:
                    link_categories(conn, pa_id, detail)
                # store nutrition
                store_nutrition(conn, mp_id, detail)
                # time-series price
                insert_price_snapshot(conn, mp_id, store_db_id, detail)

                conn.commit()
            except Exception as e:
                conn.rollback()
                print("[ERROR] DB error for product:", product_id, e)
                continue

            time.sleep(0.05)

    conn.close()
    print("[main] Done scraping.")


if __name__ == "__main__":
    main()
