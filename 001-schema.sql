-- Runs automatically on first Postgres container start (docker-entrypoint-initdb.d).
-- All SQL-backed services (customer, restaurant, order, payment) share one Postgres
-- instance for simplicity in this assignment, but each only ever touches its own tables
-- (schema-per-service by table prefix) -- no service reaches into another's tables.

-- ==================== Customer Service ====================
CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(120) NOT NULL,
    email       VARCHAR(160) NOT NULL UNIQUE,
    phone       VARCHAR(30),
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS addresses (
    id           SERIAL PRIMARY KEY,
    customer_id  INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    line1        VARCHAR(200) NOT NULL,
    city         VARCHAR(100) NOT NULL,
    is_default   BOOLEAN NOT NULL DEFAULT false,
    created_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- Local read-model of order history, kept up to date by consuming order events
-- (Customer Service does not query Order Service's DB directly).
CREATE TABLE IF NOT EXISTS customer_order_history (
    order_id     VARCHAR(64) PRIMARY KEY,
    customer_id  INTEGER NOT NULL,
    status       VARCHAR(30) NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL,
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- ==================== Restaurant Service ====================
CREATE TABLE IF NOT EXISTS restaurants (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(150) NOT NULL,
    opening_time    TIME NOT NULL DEFAULT '08:00',
    closing_time    TIME NOT NULL DEFAULT '22:00',
    is_open         BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS menu_items (
    id              SERIAL PRIMARY KEY,
    restaurant_id   INTEGER NOT NULL REFERENCES restaurants(id) ON DELETE CASCADE,
    name            VARCHAR(150) NOT NULL,
    price           NUMERIC(10,2) NOT NULL,
    in_stock        BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- ==================== Order Service ====================
CREATE TABLE IF NOT EXISTS orders (
    id             VARCHAR(64) PRIMARY KEY,
    customer_id    INTEGER NOT NULL,
    restaurant_id  INTEGER NOT NULL,
    status         VARCHAR(30) NOT NULL DEFAULT 'CREATED',
    total_amount   NUMERIC(10,2) NOT NULL,
    delivery_address VARCHAR(200),
    created_at     TIMESTAMP NOT NULL DEFAULT now(),
    updated_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    id            SERIAL PRIMARY KEY,
    order_id      VARCHAR(64) NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    menu_item_id  INTEGER NOT NULL,
    name          VARCHAR(150) NOT NULL,
    quantity      INTEGER NOT NULL,
    unit_price    NUMERIC(10,2) NOT NULL
);

-- ==================== Payment Service ====================
CREATE TABLE IF NOT EXISTS transactions (
    id             SERIAL PRIMARY KEY,
    order_id       VARCHAR(64) NOT NULL UNIQUE, -- enforces one charge attempt per order (idempotency)
    customer_id    INTEGER NOT NULL,
    amount         NUMERIC(10,2) NOT NULL,
    status         VARCHAR(20) NOT NULL, -- COMPLETED | FAILED
    failure_reason VARCHAR(200),
    created_at     TIMESTAMP NOT NULL DEFAULT now()
);

-- ==================== Seed data ====================
INSERT INTO restaurants (name, opening_time, closing_time, is_open)
VALUES ('Windhoek Grill House', '08:00', '22:00', true),
       ('Namibia Noodle Bar', '10:00', '21:00', true)
ON CONFLICT DO NOTHING;

INSERT INTO menu_items (restaurant_id, name, price, in_stock)
VALUES (1, 'Kapana Platter', 85.00, true),
       (1, 'Grilled Chicken & Chips', 95.00, true),
       (2, 'Beef Noodle Bowl', 75.00, true),
       (2, 'Vegetable Stir Fry', 65.00, true)
ON CONFLICT DO NOTHING;

INSERT INTO customers (name, email, phone)
VALUES ('Demo Customer', 'demo.customer@example.com', '+264811234567')
ON CONFLICT DO NOTHING;
