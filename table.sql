CREATE TABLE orders (
    order_id VARCHAR(255) PRIMARY KEY,
    account_id VARCHAR(255) NOT NULL,
    created TIMESTAMP NOT NULL,
    delivery_planned_moment TIMESTAMP,
    external_code VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    payed_sum NUMERIC(10, 2),
    shipment_address TEXT,
    raw_response JSONB NOT NULL
);