
-- Create Sellers Table
CREATE TABLE IF NOT EXISTS sellers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    contact_info VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20) UNIQUE
);

-- Create Houses Table
CREATE TABLE IF NOT EXISTS houses (
    id SERIAL PRIMARY KEY,
    zpid VARCHAR(20) UNIQUE,
    price VARCHAR(50),
    unformatted_price INT,
    status VARCHAR(50),
    lot_area VARCHAR(50),
    home_type VARCHAR(50),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    listing_agent VARCHAR(255),
    detail_url TEXT,
    broker_name VARCHAR(255),
    seller_id INT REFERENCES sellers(id) ON DELETE SET NULL
);

-- Create Addresses Table
CREATE TABLE IF NOT EXISTS addresses (
    id SERIAL PRIMARY KEY,
    house_id INT REFERENCES houses(id) ON DELETE CASCADE,
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zipcode VARCHAR(20)
);

-- Create House Images Table
CREATE TABLE IF NOT EXISTS house_images (
    id SERIAL PRIMARY KEY,
    house_id INT REFERENCES houses(id) ON DELETE CASCADE,
    image_url TEXT
);
