from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


PG_JDBC_URL = "jdbc:postgresql://postgres:5432/petstore"
PG_JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

CH_JDBC_URL = "jdbc:clickhouse://clickhouse:8123/default"
CH_JDBC_PROPERTIES = {
    "user": "default",
    "password":"clickhouse",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def read_pg_table(spark: SparkSession, table: str):
    return (
        spark.read.format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("dbtable", table)
        .options(**PG_JDBC_PROPERTIES)
        .load()
    )


def write_ch_table(df, table: str):
    (
        df.write.format("jdbc")
        .option("url", CH_JDBC_URL)
        .option("dbtable", table)
        .options(**CH_JDBC_PROPERTIES)
        .mode("append")
        .save()
    )


def main():
    spark = SparkSession.builder.appName("star_to_clickhouse_reports").getOrCreate()

    fact = read_pg_table(spark, "fact_sales").cache()
    dim_product = read_pg_table(spark, "dim_product").cache()
    dim_category = read_pg_table(spark, "dim_category").cache()
    dim_customer = read_pg_table(spark, "dim_customer").cache()
    dim_country = read_pg_table(spark, "dim_country").cache()
    dim_store = read_pg_table(spark, "dim_store").cache()
    dim_city = read_pg_table(spark, "dim_city").cache()
    dim_supplier = read_pg_table(spark, "dim_supplier").cache()

    sales = (
        fact.withColumn("sale_total_price", F.col("total_price").cast("double"))
        .withColumn("sale_quantity", F.col("quantity").cast("long"))
        .withColumn("sale_year", F.year(F.col("sale_date")))
        .withColumn("sale_month", F.month(F.col("sale_date")))
    )

    sales_with_product = (
        sales.join(dim_product, on="product_id", how="left")
        .join(dim_category, dim_product.category_id == dim_category.category_id, "left")
        .select(
            sales["*"],
            dim_product.name.alias("product_name"),
            dim_product.rating.cast("double").alias("product_rating"),
            dim_product.reviews.cast("long").alias("product_reviews"),
            dim_category.name.alias("product_category"),
        )
    )

    sales_with_customer = (
        sales.join(dim_customer, on="customer_id", how="left")
        .join(dim_country, dim_customer.country_id == dim_country.country_id, "left")
        .select(
            sales["*"],
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
            dim_country.name.alias("customer_country"),
        )
    )

    sales_with_store = (
        sales.join(dim_store.alias("st"), on="store_id", how="left")
        .join(dim_city.alias("city"), F.col("st.city_id") == F.col("city.city_id"), "left")
        .join(dim_country.alias("country"), F.col("city.country_id") == F.col("country.country_id"), "left")
        .select(
            sales["*"],
            F.col("st.name").alias("store_name"),
            F.col("city.name").alias("store_city"),
            F.col("country.name").alias("store_country"),
        )
    )

    sales_with_supplier = (
        sales.join(dim_supplier.alias("sp"), on="supplier_id", how="left")
        .join(dim_city.alias("city"), F.col("sp.city_id") == F.col("city.city_id"), "left")
        .join(dim_country.alias("country"), F.col("city.country_id") == F.col("country.country_id"), "left")
        .select(
            sales["*"],
            F.col("sp.name").alias("supplier_name"),
            F.col("country.name").alias("supplier_country"),
        )
    )

    top_products = (
        sales_with_product.groupBy("product_id", "product_name", "product_category")
        .agg(F.sum("sale_quantity").alias("total_sold"))
        .orderBy(F.col("total_sold").desc())
        .limit(10)
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("total_sold").desc())))
        .select(
            F.lit("top10_sold").alias("metric_type"),
            F.col("product_id").cast("long").alias("product_id"),
            "product_name",
            "product_category",
            F.col("total_sold").cast("long").alias("total_sold"),
            F.lit(None).cast("double").alias("revenue"),
            F.lit(None).cast("double").alias("avg_rating"),
            F.lit(None).cast("long").alias("total_reviews"),
            F.col("rank").cast("int").alias("rank"),
        )
    )

    revenue_by_category = (
        sales_with_product.groupBy("product_category")
        .agg(F.round(F.sum("sale_total_price"), 2).alias("revenue"))
        .select(
            F.lit("revenue_by_category").alias("metric_type"),
            F.lit(None).cast("long").alias("product_id"),
            F.lit(None).cast("string").alias("product_name"),
            "product_category",
            F.lit(None).cast("long").alias("total_sold"),
            F.col("revenue").cast("double").alias("revenue"),
            F.lit(None).cast("double").alias("avg_rating"),
            F.lit(None).cast("long").alias("total_reviews"),
            F.lit(None).cast("int").alias("rank"),
        )
    )

    rating_by_product = (
        sales_with_product.groupBy("product_id", "product_name", "product_category")
        .agg(
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.sum("product_reviews").alias("total_reviews"),
        )
        .select(
            F.lit("rating_reviews").alias("metric_type"),
            F.col("product_id").cast("long").alias("product_id"),
            "product_name",
            "product_category",
            F.lit(None).cast("long").alias("total_sold"),
            F.lit(None).cast("double").alias("revenue"),
            F.col("avg_rating").cast("double").alias("avg_rating"),
            F.col("total_reviews").cast("long").alias("total_reviews"),
            F.lit(None).cast("int").alias("rank"),
        )
    )

    report_products = top_products.unionByName(revenue_by_category).unionByName(rating_by_product)
    write_ch_table(report_products, "report_products")

    top_customers = (
        sales_with_customer.groupBy("customer_id", "customer_name", "customer_country")
        .agg(F.round(F.sum("sale_total_price"), 2).alias("total_revenue"))
        .orderBy(F.col("total_revenue").desc())
        .limit(10)
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("total_revenue").desc())))
        .select(
            F.lit("top10_by_revenue").alias("metric_type"),
            F.col("customer_id").cast("long").alias("customer_id"),
            "customer_name",
            "customer_country",
            F.col("total_revenue").cast("double").alias("total_revenue"),
            F.lit(None).cast("long").alias("customers_count"),
            F.lit(None).cast("double").alias("avg_check"),
            F.col("rank").cast("int").alias("rank"),
        )
    )

    customers_by_country = (
        sales_with_customer.groupBy("customer_country")
        .agg(F.countDistinct("customer_id").alias("customers_count"))
        .select(
            F.lit("distribution_by_country").alias("metric_type"),
            F.lit(None).cast("long").alias("customer_id"),
            F.lit(None).cast("string").alias("customer_name"),
            "customer_country",
            F.lit(None).cast("double").alias("total_revenue"),
            F.col("customers_count").cast("long").alias("customers_count"),
            F.lit(None).cast("double").alias("avg_check"),
            F.lit(None).cast("int").alias("rank"),
        )
    )

    customer_avg_check = (
        sales_with_customer.groupBy("customer_id", "customer_name", "customer_country")
        .agg(F.round(F.avg("sale_total_price"), 2).alias("avg_check"))
        .select(
            F.lit("avg_check_per_customer").alias("metric_type"),
            F.col("customer_id").cast("long").alias("customer_id"),
            "customer_name",
            "customer_country",
            F.lit(None).cast("double").alias("total_revenue"),
            F.lit(None).cast("long").alias("customers_count"),
            F.col("avg_check").cast("double").alias("avg_check"),
            F.lit(None).cast("int").alias("rank"),
        )
    )

    report_customers = top_customers.unionByName(customers_by_country).unionByName(customer_avg_check)
    write_ch_table(report_customers, "report_customers")

    monthly_revenue = (
        sales.groupBy("sale_year", "sale_month")
        .agg(F.round(F.sum("sale_total_price"), 2).alias("total_revenue"))
        .select(
            F.lit("monthly_revenue").alias("metric_type"),
            F.col("sale_year").cast("int").alias("year"),
            F.col("sale_month").cast("int").alias("month"),
            F.col("total_revenue").cast("double").alias("total_revenue"),
            F.lit(None).cast("double").alias("avg_order"),
        )
    )

    yearly_revenue = (
        sales.groupBy("sale_year")
        .agg(F.round(F.sum("sale_total_price"), 2).alias("total_revenue"))
        .select(
            F.lit("yearly_revenue").alias("metric_type"),
            F.col("sale_year").cast("int").alias("year"),
            F.lit(None).cast("int").alias("month"),
            F.col("total_revenue").cast("double").alias("total_revenue"),
            F.lit(None).cast("double").alias("avg_order"),
        )
    )

    monthly_avg_order = (
        sales.groupBy("sale_year", "sale_month")
        .agg(F.round(F.avg("sale_total_price"), 2).alias("avg_order"))
        .select(
            F.lit("monthly_avg_order").alias("metric_type"),
            F.col("sale_year").cast("int").alias("year"),
            F.col("sale_month").cast("int").alias("month"),
            F.lit(None).cast("double").alias("total_revenue"),
            F.col("avg_order").cast("double").alias("avg_order"),
        )
    )

    report_time = monthly_revenue.unionByName(yearly_revenue).unionByName(monthly_avg_order)
    write_ch_table(report_time, "report_time")

    top_stores = (
        sales_with_store.groupBy("store_id", "store_name")
        .agg(F.round(F.sum("sale_total_price"), 2).alias("total_revenue"))
        .orderBy(F.col("total_revenue").desc())
        .limit(5)
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("total_revenue").desc())))
        .select(
            F.lit("top5_by_revenue").alias("metric_type"),
            F.col("store_id").cast("long").alias("store_id"),
            "store_name",
            F.lit(None).cast("string").alias("store_city"),
            F.lit(None).cast("string").alias("store_country"),
            F.lit(None).cast("long").alias("orders_count"),
            F.col("total_revenue").cast("double").alias("total_revenue"),
            F.lit(None).cast("double").alias("avg_check"),
            F.col("rank").cast("int").alias("rank"),
        )
    )

    sales_by_city_country = (
        sales_with_store.groupBy("store_city", "store_country")
        .agg(
            F.count("sale_id").alias("orders_count"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        )
        .select(
            F.lit("sales_by_city_country").alias("metric_type"),
            F.lit(None).cast("long").alias("store_id"),
            F.lit(None).cast("string").alias("store_name"),
            "store_city",
            "store_country",
            F.col("orders_count").cast("long").alias("orders_count"),
            F.col("total_revenue").cast("double").alias("total_revenue"),
            F.lit(None).cast("double").alias("avg_check"),
            F.lit(None).cast("int").alias("rank"),
        )
    )

    store_avg_check = (
        sales_with_store.groupBy("store_id", "store_name")
        .agg(F.round(F.avg("sale_total_price"), 2).alias("avg_check"))
        .select(
            F.lit("avg_check_per_store").alias("metric_type"),
            F.col("store_id").cast("long").alias("store_id"),
            "store_name",
            F.lit(None).cast("string").alias("store_city"),
            F.lit(None).cast("string").alias("store_country"),
            F.lit(None).cast("long").alias("orders_count"),
            F.lit(None).cast("double").alias("total_revenue"),
            F.col("avg_check").cast("double").alias("avg_check"),
            F.lit(None).cast("int").alias("rank"),
        )
    )

    report_stores = top_stores.unionByName(sales_by_city_country).unionByName(store_avg_check)
    write_ch_table(report_stores, "report_stores")

    top_suppliers = (
        sales_with_supplier.groupBy("supplier_id", "supplier_name")
        .agg(F.round(F.sum("sale_total_price"), 2).alias("total_revenue"))
        .orderBy(F.col("total_revenue").desc())
        .limit(5)
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("total_revenue").desc())))
        .select(
            F.lit("top5_by_revenue").alias("metric_type"),
            F.col("supplier_id").cast("long").alias("supplier_id"),
            "supplier_name",
            F.lit(None).cast("string").alias("supplier_country"),
            F.col("total_revenue").cast("double").alias("total_revenue"),
            F.lit(None).cast("double").alias("avg_unit_price"),
            F.col("rank").cast("int").alias("rank"),
        )
    )

    avg_unit_price_by_supplier = (
        sales_with_supplier.filter(F.col("sale_quantity") > 0)
        .withColumn("unit_price", F.col("sale_total_price") / F.col("sale_quantity"))
        .groupBy("supplier_id", "supplier_name")
        .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
        .select(
            F.lit("avg_unit_price").alias("metric_type"),
            F.col("supplier_id").cast("long").alias("supplier_id"),
            "supplier_name",
            F.lit(None).cast("string").alias("supplier_country"),
            F.lit(None).cast("double").alias("total_revenue"),
            F.col("avg_unit_price").cast("double").alias("avg_unit_price"),
            F.lit(None).cast("int").alias("rank"),
        )
    )

    revenue_by_supplier_country = (
        sales_with_supplier.groupBy("supplier_country")
        .agg(F.round(F.sum("sale_total_price"), 2).alias("total_revenue"))
        .select(
            F.lit("revenue_by_country").alias("metric_type"),
            F.lit(None).cast("long").alias("supplier_id"),
            F.lit(None).cast("string").alias("supplier_name"),
            "supplier_country",
            F.col("total_revenue").cast("double").alias("total_revenue"),
            F.lit(None).cast("double").alias("avg_unit_price"),
            F.lit(None).cast("int").alias("rank"),
        )
    )

    report_suppliers = top_suppliers.unionByName(avg_unit_price_by_supplier).unionByName(revenue_by_supplier_country)
    write_ch_table(report_suppliers, "report_suppliers")

    quality_base = (
        sales_with_product.groupBy("product_id", "product_name")
        .agg(
            F.round(F.avg("product_rating"), 2).alias("product_rating"),
            F.sum("sale_quantity").cast("long").alias("total_sold"),
            F.sum("product_reviews").cast("long").alias("total_reviews"),
        )
        .cache()
    )

    max_rating = (
        quality_base.orderBy(F.col("product_rating").desc_nulls_last())
        .limit(1)
        .select(
            F.lit("rating_extreme").alias("metric_type"),
            F.col("product_id").cast("long").alias("product_id"),
            "product_name",
            F.col("product_rating").cast("double").alias("product_rating"),
            "total_sold",
            "total_reviews",
            F.lit(None).cast("double").alias("corr_rating_sales"),
            F.lit("max").alias("extreme_type"),
            F.lit(1).cast("int").alias("rank"),
        )
    )

    min_rating = (
        quality_base.orderBy(F.col("product_rating").asc_nulls_last())
        .limit(1)
        .select(
            F.lit("rating_extreme").alias("metric_type"),
            F.col("product_id").cast("long").alias("product_id"),
            "product_name",
            F.col("product_rating").cast("double").alias("product_rating"),
            "total_sold",
            "total_reviews",
            F.lit(None).cast("double").alias("corr_rating_sales"),
            F.lit("min").alias("extreme_type"),
            F.lit(2).cast("int").alias("rank"),
        )
    )

    corr_value = quality_base.select(F.corr("product_rating", "total_sold").alias("corr_rating_sales"))
    corr_report = corr_value.select(
        F.lit("rating_sales_correlation").alias("metric_type"),
        F.lit(None).cast("long").alias("product_id"),
        F.lit(None).cast("string").alias("product_name"),
        F.lit(None).cast("double").alias("product_rating"),
        F.lit(None).cast("long").alias("total_sold"),
        F.lit(None).cast("long").alias("total_reviews"),
        F.col("corr_rating_sales").cast("double").alias("corr_rating_sales"),
        F.lit(None).cast("string").alias("extreme_type"),
        F.lit(None).cast("int").alias("rank"),
    )

    top_reviews = (
        quality_base.orderBy(F.col("total_reviews").desc_nulls_last())
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("total_reviews").desc_nulls_last())))
        .limit(10)
        .select(
            F.lit("top_reviews").alias("metric_type"),
            F.col("product_id").cast("long").alias("product_id"),
            "product_name",
            F.col("product_rating").cast("double").alias("product_rating"),
            "total_sold",
            "total_reviews",
            F.lit(None).cast("double").alias("corr_rating_sales"),
            F.lit(None).cast("string").alias("extreme_type"),
            F.col("rank").cast("int").alias("rank"),
        )
    )

    report_quality = max_rating.unionByName(min_rating).unionByName(corr_report).unionByName(top_reviews)
    write_ch_table(report_quality, "report_quality")

    spark.stop()


if __name__ == "__main__":
    main()
