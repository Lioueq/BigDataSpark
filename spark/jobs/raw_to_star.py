from pyspark.sql import SparkSession
from pyspark.sql import functions as F


JDBC_URL = "jdbc:postgresql://postgres:5432/petstore"
JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}


def read_table(spark: SparkSession, table: str):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .options(**JDBC_PROPERTIES)
        .load()
    )


def append_table(df, table: str):
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .options(**JDBC_PROPERTIES)
        .mode("append")
        .save()
    )


def sk_from_file_and_id(file_id_col, id_col):
    return file_id_col.cast("int") * F.lit(100000) + id_col.cast("int")


def stable_positive_hash(cols):
    joined = F.concat_ws("|", *[F.coalesce(c, F.lit("")) for c in cols])
    return F.pmod(F.abs(F.hash(joined)), F.lit(2147483647))


def not_empty(col_name: str):
    return F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != "")


def empty_to_null(col_name: str):
    return F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name))


def parse_us_date(col_name: str):
    value = empty_to_null(col_name)
    return F.coalesce(
        F.to_date(value, "MM/dd/yyyy"),
        F.to_date(value, "M/d/yyyy"),
        F.to_date(value, "M/dd/yyyy"),
        F.to_date(value, "MM/d/yyyy"),
    )


def main():
    spark = (
        SparkSession.builder.appName("raw_to_star")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

    raw = read_table(spark, "raw_data").cache()

    country_customer = raw.filter(not_empty("customer_country")).select(
        F.col("customer_country").alias("name")
    )
    country_seller = raw.filter(not_empty("seller_country")).select(
        F.col("seller_country").alias("name")
    )
    country_store = raw.filter(not_empty("store_country")).select(
        F.col("store_country").alias("name")
    )
    country_supplier = raw.filter(not_empty("supplier_country")).select(
        F.col("supplier_country").alias("name")
    )

    dim_country_new = (
        country_customer.unionByName(country_seller)
        .unionByName(country_store)
        .unionByName(country_supplier)
        .dropDuplicates(["name"])
    )

    dim_country_existing = read_table(spark, "dim_country").select("name")
    dim_country_to_insert = dim_country_new.join(dim_country_existing, on="name", how="left_anti")
    append_table(dim_country_to_insert, "dim_country")

    dim_country = read_table(spark, "dim_country").select("country_id", "name").cache()

    store_city_src = (
        raw.filter(not_empty("store_city"))
        .select(
            F.coalesce(empty_to_null("store_city"), F.lit("(unknown)")).alias("name"),
            F.coalesce(empty_to_null("store_state"), F.lit("(unknown)")).alias("state"),
            F.coalesce(empty_to_null("store_country"), F.lit("(unknown)")).alias("country_name"),
        )
    )

    supplier_city_src = (
        raw.filter(not_empty("supplier_city"))
        .select(
            F.coalesce(empty_to_null("supplier_city"), F.lit("(unknown)")).alias("name"),
            F.lit("(unknown)").alias("state"),
            F.coalesce(empty_to_null("supplier_country"), F.lit("(unknown)")).alias("country_name"),
        )
    )

    dim_city_new = (
        store_city_src.unionByName(supplier_city_src)
        .alias("src")
        .join(dim_country.alias("c"), F.col("src.country_name") == F.col("c.name"), "inner")
        .select(
            F.col("src.name").alias("name"),
            F.col("src.state").alias("state"),
            F.col("c.country_id").alias("country_id"),
        )
        .dropDuplicates(["name", "state", "country_id"])
    )

    dim_city_existing = read_table(spark, "dim_city").select("name", "state", "country_id")
    dim_city_to_insert = dim_city_new.join(
        dim_city_existing,
        on=["name", "state", "country_id"],
        how="left_anti",
    )
    append_table(dim_city_to_insert, "dim_city")

    dim_city = read_table(spark, "dim_city").select("city_id", "name", "state", "country_id").cache()

    dim_category_new = (
        raw.filter(not_empty("product_category"))
        .select(F.col("product_category").alias("name"))
        .dropDuplicates(["name"])
    )

    dim_category_existing = read_table(spark, "dim_category").select("name")
    dim_category_to_insert = dim_category_new.join(dim_category_existing, on="name", how="left_anti")
    append_table(dim_category_to_insert, "dim_category")

    dim_category = read_table(spark, "dim_category").select("category_id", "name").cache()

    dim_customer_new = (
        raw.filter(not_empty("sale_customer_id"))
        .withColumn("customer_id", sk_from_file_and_id(F.col("file_id"), F.col("sale_customer_id")))
        .join(dim_country, empty_to_null("customer_country") == dim_country.name, "left")
        .select(
            "customer_id",
            F.col("customer_first_name").alias("first_name"),
            F.col("customer_last_name").alias("last_name"),
            empty_to_null("customer_age").cast("int").alias("age"),
            F.col("customer_email").alias("email"),
            F.col("customer_postal_code").alias("postal_code"),
            F.col("customer_pet_type").alias("pet_type"),
            F.col("customer_pet_name").alias("pet_name"),
            F.col("customer_pet_breed").alias("pet_breed"),
            "country_id",
        )
        .dropDuplicates(["customer_id"])
    )

    dim_customer_existing = read_table(spark, "dim_customer").select("customer_id")
    dim_customer_to_insert = dim_customer_new.join(dim_customer_existing, on="customer_id", how="left_anti")
    append_table(dim_customer_to_insert, "dim_customer")

    dim_seller_new = (
        raw.filter(not_empty("sale_seller_id"))
        .withColumn("seller_id", sk_from_file_and_id(F.col("file_id"), F.col("sale_seller_id")))
        .join(dim_country, empty_to_null("seller_country") == dim_country.name, "left")
        .select(
            "seller_id",
            F.col("seller_first_name").alias("first_name"),
            F.col("seller_last_name").alias("last_name"),
            F.col("seller_email").alias("email"),
            F.col("seller_postal_code").alias("postal_code"),
            "country_id",
        )
        .dropDuplicates(["seller_id"])
    )

    dim_seller_existing = read_table(spark, "dim_seller").select("seller_id")
    dim_seller_to_insert = dim_seller_new.join(dim_seller_existing, on="seller_id", how="left_anti")
    append_table(dim_seller_to_insert, "dim_seller")

    dim_product_new = (
        raw.filter(not_empty("sale_product_id"))
        .withColumn("product_id", sk_from_file_and_id(F.col("file_id"), F.col("sale_product_id")))
        .join(dim_category, empty_to_null("product_category") == dim_category.name, "left")
        .select(
            "product_id",
            F.col("product_name").alias("name"),
            "pet_category",
            empty_to_null("product_weight").cast("decimal(10,2)").alias("weight"),
            F.col("product_color").alias("color"),
            F.col("product_size").alias("size"),
            F.col("product_brand").alias("brand"),
            F.col("product_material").alias("material"),
            F.col("product_description").alias("description"),
            empty_to_null("product_rating").cast("decimal(3,2)").alias("rating"),
            empty_to_null("product_reviews").cast("int").alias("reviews"),
            parse_us_date("product_release_date").alias("release_date"),
            parse_us_date("product_expiry_date").alias("expiry_date"),
            "category_id",
        )
        .dropDuplicates(["product_id"])
    )

    dim_product_existing = read_table(spark, "dim_product").select("product_id")
    dim_product_to_insert = dim_product_new.join(dim_product_existing, on="product_id", how="left_anti")
    append_table(dim_product_to_insert, "dim_product")

    store_id_expr = stable_positive_hash(
        [
            F.col("store_name"),
            F.col("store_location"),
            F.col("store_city"),
            F.col("store_state"),
            F.col("store_country"),
            F.col("store_phone"),
            F.col("store_email"),
        ]
    )

    dim_store_new = (
        raw.filter(not_empty("store_name"))
        .withColumn("store_id", store_id_expr)
        .withColumn("country_name", F.coalesce(empty_to_null("store_country"), F.lit("(unknown)")))
        .withColumn("city_name", F.coalesce(empty_to_null("store_city"), F.lit("(unknown)")))
        .withColumn("state_name", F.coalesce(empty_to_null("store_state"), F.lit("(unknown)")))
        .join(dim_country.alias("co"), F.col("country_name") == F.col("co.name"), "left")
        .join(
            dim_city.alias("dc"),
            (F.col("city_name") == F.col("dc.name"))
            & (F.col("state_name") == F.col("dc.state"))
            & (F.col("co.country_id") == F.col("dc.country_id")),
            "left",
        )
        .select(
            "store_id",
            F.col("store_name").alias("name"),
            F.col("store_location").alias("location"),
            F.col("store_phone").alias("phone"),
            F.col("store_email").alias("email"),
            F.col("dc.city_id").alias("city_id"),
        )
        .dropDuplicates(["store_id"])
    )

    dim_store_existing = read_table(spark, "dim_store").select("store_id")
    dim_store_to_insert = dim_store_new.join(dim_store_existing, on="store_id", how="left_anti")
    append_table(dim_store_to_insert, "dim_store")

    supplier_id_expr = stable_positive_hash(
        [
            F.col("supplier_name"),
            F.col("supplier_contact"),
            F.col("supplier_email"),
            F.col("supplier_phone"),
            F.col("supplier_address"),
            F.col("supplier_city"),
            F.col("supplier_country"),
        ]
    )

    dim_supplier_new = (
        raw.filter(not_empty("supplier_name"))
        .withColumn("supplier_id", supplier_id_expr)
        .withColumn("country_name", F.coalesce(empty_to_null("supplier_country"), F.lit("(unknown)")))
        .withColumn("city_name", F.coalesce(empty_to_null("supplier_city"), F.lit("(unknown)")))
        .join(dim_country.alias("co"), F.col("country_name") == F.col("co.name"), "left")
        .join(
            dim_city.alias("dc"),
            (F.col("city_name") == F.col("dc.name"))
            & (F.col("dc.state") == F.lit("(unknown)"))
            & (F.col("co.country_id") == F.col("dc.country_id")),
            "left",
        )
        .select(
            "supplier_id",
            F.col("supplier_name").alias("name"),
            F.col("supplier_contact").alias("contact"),
            F.col("supplier_email").alias("email"),
            F.col("supplier_phone").alias("phone"),
            F.col("supplier_address").alias("address"),
            F.col("dc.city_id").alias("city_id"),
        )
        .dropDuplicates(["supplier_id"])
    )

    dim_supplier_existing = read_table(spark, "dim_supplier").select("supplier_id")
    dim_supplier_to_insert = dim_supplier_new.join(dim_supplier_existing, on="supplier_id", how="left_anti")
    append_table(dim_supplier_to_insert, "dim_supplier")

    fact_sales_new = (
        raw.filter(not_empty("id"))
        .select(
            sk_from_file_and_id(F.col("file_id"), F.col("id")).alias("sale_id"),
            sk_from_file_and_id(F.col("file_id"), F.col("sale_customer_id")).alias("customer_id"),
            sk_from_file_and_id(F.col("file_id"), F.col("sale_seller_id")).alias("seller_id"),
            sk_from_file_and_id(F.col("file_id"), F.col("sale_product_id")).alias("product_id"),
            store_id_expr.alias("store_id"),
            supplier_id_expr.alias("supplier_id"),
            parse_us_date("sale_date").alias("sale_date"),
            empty_to_null("sale_quantity").cast("int").alias("quantity"),
            empty_to_null("sale_total_price").cast("decimal(15,2)").alias("total_price"),
        )
        .dropDuplicates(["sale_id"])
    )

    fact_sales_existing = read_table(spark, "fact_sales").select("sale_id")
    fact_sales_to_insert = fact_sales_new.join(fact_sales_existing, on="sale_id", how="left_anti")
    append_table(fact_sales_to_insert, "fact_sales")

    spark.stop()


if __name__ == "__main__":
    main()
