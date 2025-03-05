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
    zpid VARCHAR(20) PRIMARY KEY,
    price INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    area VARCHAR(50),
    home_type VARCHAR(50) NOT NULL,
    latitude DECIMAL(10, 6) NOT NULL,
    longitude DECIMAL(10, 6) NOT NULL,
    listing_agent VARCHAR(255),
    detail_url TEXT,
    broker_name VARCHAR(255),
    seller_id INT REFERENCES sellers(id) ON DELETE SET NULL
);

-- Create Addresses Table
CREATE TABLE IF NOT EXISTS addresses (
    id SERIAL PRIMARY KEY,
    house_zpid VARCHAR(20) UNIQUE REFERENCES houses(zpid) ON DELETE CASCADE,
    street VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    zipcode VARCHAR(20) NOT NULL
);

-- Create House Images Table
CREATE TABLE IF NOT EXISTS house_images (
    id SERIAL PRIMARY KEY,
    house_zpid VARCHAR(20) REFERENCES houses(zpid) ON DELETE CASCADE,
    image_url TEXT NOT NULL
);
