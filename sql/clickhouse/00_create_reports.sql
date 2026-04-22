CREATE TABLE IF NOT EXISTS report_products (
    metric_type String,
    product_id Nullable(Int64),
    product_name Nullable(String),
    product_category Nullable(String),
    total_sold Nullable(Int64),
    revenue Nullable(Float64),
    avg_rating Nullable(Float64),
    total_reviews Nullable(Int64),
    rank Nullable(Int32),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (metric_type, ifNull(product_id, 0), ifNull(rank, 0));

CREATE TABLE IF NOT EXISTS report_customers (
    metric_type String,
    customer_id Nullable(Int64),
    customer_name Nullable(String),
    customer_country Nullable(String),
    total_revenue Nullable(Float64),
    customers_count Nullable(Int64),
    avg_check Nullable(Float64),
    rank Nullable(Int32),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (metric_type, ifNull(customer_id, 0), ifNull(rank, 0));

CREATE TABLE IF NOT EXISTS report_time (
    metric_type String,
    year Nullable(Int32),
    month Nullable(Int32),
    total_revenue Nullable(Float64),
    avg_order Nullable(Float64),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (metric_type, ifNull(year, 0), ifNull(month, 0));

CREATE TABLE IF NOT EXISTS report_stores (
    metric_type String,
    store_id Nullable(Int64),
    store_name Nullable(String),
    store_city Nullable(String),
    store_country Nullable(String),
    orders_count Nullable(Int64),
    total_revenue Nullable(Float64),
    avg_check Nullable(Float64),
    rank Nullable(Int32),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (metric_type, ifNull(store_id, 0), ifNull(rank, 0));

CREATE TABLE IF NOT EXISTS report_suppliers (
    metric_type String,
    supplier_id Nullable(Int64),
    supplier_name Nullable(String),
    supplier_country Nullable(String),
    total_revenue Nullable(Float64),
    avg_unit_price Nullable(Float64),
    rank Nullable(Int32),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (metric_type, ifNull(supplier_id, 0), ifNull(rank, 0));

CREATE TABLE IF NOT EXISTS report_quality (
    metric_type String,
    product_id Nullable(Int64),
    product_name Nullable(String),
    product_rating Nullable(Float64),
    total_sold Nullable(Int64),
    total_reviews Nullable(Int64),
    corr_rating_sales Nullable(Float64),
    extreme_type Nullable(String),
    rank Nullable(Int32),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (metric_type, ifNull(product_id, 0), ifNull(rank, 0));
