CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    -- NULL for guests (POST /session)
    username VARCHAR(255) UNIQUE,
    password_hash VARCHAR(255),
    is_guest BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (is_guest OR (username IS NOT NULL AND password_hash IS NOT NULL))
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(64) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    image_url VARCHAR(1024),
    price_cents INTEGER NOT NULL CHECK (price_cents >= 0),
    max_per_user INTEGER NOT NULL DEFAULT 1 CHECK (max_per_user > 0),
    -- When the waiting room starts admitting. Joins before this wait in a lobby
    -- and are shuffled into the queue when the sale opens.
    sale_opens_at TIMESTAMPTZ NOT NULL,
    sale_ends_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'SCHEDULED'
        CHECK (status IN ('SCHEDULED', 'ON_SALE', 'CLOSED', 'CANCELED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (sale_opens_at < sale_ends_at)
);

CREATE INDEX products_sale_opens_at_idx ON products (sale_opens_at);

-- One row per product. `stock` is the units not yet sold; it only goes down
-- in the payment worker's conditional update (holds live in Redis).
CREATE TABLE inventory (
    product_id INTEGER PRIMARY KEY REFERENCES products(id),
    stock_total INTEGER NOT NULL CHECK (stock_total >= 0),
    stock INTEGER NOT NULL CHECK (stock >= 0 AND stock <= stock_total),
    version INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

SELECT diesel_manage_updated_at('inventory');
