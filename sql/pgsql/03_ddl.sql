CREATE TABLE dim_country (
    country_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_city (
    city_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    state VARCHAR(100),
    country_id INTEGER REFERENCES dim_country(country_id),
    UNIQUE (name, state, country_id)
);

CREATE TABLE dim_category (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INTEGER,
    email VARCHAR(200),
    postal_code VARCHAR(50),
    pet_type VARCHAR(100),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(100),
    country_id INTEGER REFERENCES dim_country(country_id)
);

CREATE TABLE dim_seller (
    seller_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    postal_code VARCHAR(50),
    country_id INTEGER REFERENCES dim_country(country_id)
);

CREATE TABLE dim_product (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    pet_category VARCHAR(100),
    weight NUMERIC(10,2),
    color VARCHAR(100),
    size VARCHAR(100),
    brand VARCHAR(100),
    material VARCHAR(100),
    description TEXT,
    rating NUMERIC(3,2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE,
    category_id INTEGER REFERENCES dim_category(category_id)
);

CREATE TABLE dim_store (
    store_id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    location VARCHAR(200),
    phone VARCHAR(50),
    email VARCHAR(200),
    city_id INTEGER REFERENCES dim_city(city_id)
);

CREATE TABLE dim_supplier (
    supplier_id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    contact VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    address VARCHAR(200),
    city_id INTEGER REFERENCES dim_city(city_id)
);

CREATE TABLE fact_sales (
    sale_id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES dim_customer(customer_id),
    seller_id INTEGER REFERENCES dim_seller(seller_id),
    product_id INTEGER REFERENCES dim_product(product_id),
    store_id INTEGER REFERENCES dim_store(store_id),
    supplier_id INTEGER REFERENCES dim_supplier(supplier_id),
    sale_date DATE,
    quantity INTEGER,
    total_price NUMERIC(15,2)
);
