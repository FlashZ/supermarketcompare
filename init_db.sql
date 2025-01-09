-- ------------------------------------------------------------------
-- master_products Table
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS master_products (
  id SERIAL PRIMARY KEY,
  barcode TEXT UNIQUE,
  brand TEXT,
  product_name TEXT NOT NULL,
  package_size TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP
);

-- ------------------------------------------------------------------
-- product_aliases Table
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_aliases (
  id SERIAL PRIMARY KEY,
  master_product_id INT NOT NULL REFERENCES master_products(id) ON DELETE CASCADE,
  retailer_name TEXT NOT NULL,
  retailer_sku TEXT NOT NULL,
  display_name TEXT,
  extra_json JSONB,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP
);

-- Create Unique Constraint on (retailer_name, retailer_sku)
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

-- ------------------------------------------------------------------
-- stores Table
-- ------------------------------------------------------------------
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

-- ------------------------------------------------------------------
-- prices Table
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prices (
  id SERIAL PRIMARY KEY,
  master_product_id INT NOT NULL REFERENCES master_products(id) ON DELETE CASCADE,
  store_id INT NOT NULL REFERENCES stores(id) ON DELETE CASCADE,

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

-- ------------------------------------------------------------------
-- categories Table
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS categories (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  parent_id INT REFERENCES categories(id) ON DELETE CASCADE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP,
  UNIQUE (name, parent_id)
);

-- ------------------------------------------------------------------
-- product_categories Table
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_categories (
  product_alias_id INT NOT NULL REFERENCES product_aliases(id) ON DELETE CASCADE,
  category_id INT NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
  PRIMARY KEY (product_alias_id, category_id)
);

-- ------------------------------------------------------------------
-- product_nutrition Table
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_nutrition (
  id SERIAL PRIMARY KEY,
  master_product_id INT NOT NULL REFERENCES master_products(id) ON DELETE CASCADE,
  nutrient_code TEXT,
  nutrient_name TEXT NOT NULL,
  amount NUMERIC(10,2),
  unit TEXT,
  basis_qty NUMERIC(10,2),
  measurement_precision TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP
);

-- ------------------------------------------------------------------
-- Indexes for Optimization (Optional)
-- ------------------------------------------------------------------

-- Index on product_aliases.retailer_sku for faster lookups
CREATE INDEX IF NOT EXISTS idx_product_aliases_retailer_sku ON product_aliases(retailer_sku);



