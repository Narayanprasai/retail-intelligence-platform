import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, lower, upper, trim, round,
    to_date, year, month, quarter,
    dayofweek, date_format,
    when, lit, datediff,
    md5, concat_ws,
    sum as spark_sum,
    avg as spark_avg
)
from pyspark.sql.types import IntegerType, DoubleType
import logging

# =========================
# SETUP
# =========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'curated_bucket'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

CURATED_BUCKET = args['curated_bucket']
STAR_PATH = f"s3://{CURATED_BUCKET}/star_schema"

logger.info(f"Starting star_schema_builder job")
logger.info(f"Curated bucket: {CURATED_BUCKET}")
logger.info(f"Star schema path: {STAR_PATH}")

# =========================
# HELPER FUNCTIONS
# =========================
def read_curated(table_name):
    """Read clean Parquet from curated zone"""
    path = f"s3://{CURATED_BUCKET}/{table_name}/"
    logger.info(f"Reading curated/{table_name}")
    return spark.read.parquet(path)

def write_dimension(df, name):
    """Write dimension table to star schema zone"""
    path = f"{STAR_PATH}/{name}/"
    logger.info(f"Writing {name} → {path}")
    df.write.mode("overwrite").parquet(path)
    logger.info(f"✅ {name}: {df.count()} rows")

def write_fact(df, name, partition_cols=None):
    """Write fact table to star schema zone"""
    path = f"{STAR_PATH}/{name}/"
    logger.info(f"Writing {name} → {path}")
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(partition_cols)
    writer.save(path)
    logger.info(f"✅ {name}: {df.count()} rows")

# =========================
# READ ALL CURATED TABLES
# =========================
def read_all_tables():
    logger.info("Reading all curated tables...")
    orders      = read_curated("orders")
    customers   = read_curated("customers")
    order_items = read_curated("order_items")
    products    = read_curated("products")
    sellers     = read_curated("sellers")
    payments    = read_curated("payments")
    reviews     = read_curated("reviews")
    return orders, customers, order_items, products, sellers, payments, reviews

# =========================
# BUILD dim_customer
# =========================
def build_dim_customer(customers):
    logger.info("Building dim_customer...")

    dim = customers.select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state"
    ).dropDuplicates(["customer_id"])

    # Generate stable surrogate key using MD5 hash
    dim = dim.withColumn(
        "customer_sk",
        md5(col("customer_id"))
    )

    # SCD Type 2 columns — initial load all current
    dim = dim.withColumn("valid_from", lit("2016-01-01"))
    dim = dim.withColumn("valid_to", lit(None).cast("string"))
    dim = dim.withColumn("is_current", lit(True))

    # Final column order
    dim = dim.select(
        "customer_sk",
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "valid_from",
        "valid_to",
        "is_current"
    )

    return dim

# =========================
# BUILD dim_product
# =========================
def build_dim_product(products):
    logger.info("Building dim_product...")

    dim = products.select(
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_photos_qty"
    ).dropDuplicates(["product_id"])

    # Generate stable surrogate key
    dim = dim.withColumn(
        "product_sk",
        md5(col("product_id"))
    )

    dim = dim.select(
        "product_sk",
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_photos_qty"
    )

    return dim

# =========================
# BUILD dim_seller
# =========================
def build_dim_seller(sellers):
    logger.info("Building dim_seller...")

    dim = sellers.select(
        "seller_id",
        "seller_city",
        "seller_state",
        "seller_zip_code_prefix"
    ).dropDuplicates(["seller_id"])

    # Generate stable surrogate key
    dim = dim.withColumn(
        "seller_sk",
        md5(col("seller_id"))
    )

    dim = dim.select(
        "seller_sk",
        "seller_id",
        "seller_city",
        "seller_state",
        "seller_zip_code_prefix"
    )

    return dim

# =========================
# BUILD dim_date
# =========================
def build_dim_date(orders):
    logger.info("Building dim_date...")

    # Extract all unique dates from orders
    dates = orders.select(
        to_date(col("order_purchase_timestamp")).alias("full_date")
    ).dropDuplicates(["full_date"]) \
     .dropna(subset=["full_date"])

    # Add date attributes
    dim = dates \
        .withColumn("year",         year(col("full_date"))) \
        .withColumn("month",        month(col("full_date"))) \
        .withColumn("quarter",      quarter(col("full_date"))) \
        .withColumn("day_of_week",  dayofweek(col("full_date"))) \
        .withColumn("month_name",   date_format(col("full_date"), "MMMM")) \
        .withColumn("day_name",     date_format(col("full_date"), "EEEE")) \
        .withColumn("is_weekend",
            when(dayofweek(col("full_date")).isin([1, 7]), True)
            .otherwise(False)
        )

    # Generate stable surrogate key from date string
    dim = dim.withColumn(
        "date_id",
        md5(col("full_date").cast("string"))
    )

    dim = dim.select(
        "date_id",
        "full_date",
        "year",
        "month",
        "quarter",
        "day_of_week",
        "day_name",
        "month_name",
        "is_weekend"
    )

    return dim

# =========================
# BUILD fact_order_items
# =========================
def build_fact_order_items(
    orders, order_items, customers, products,
    sellers, payments, reviews,
    dim_customer, dim_product, dim_seller, dim_date
):
    logger.info("Building fact_order_items...")

    # Start with order_items — our grain
    fact = order_items.alias("oi")

    # Join orders to get customer_id and dates
    fact = fact.join(
        orders.select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ).alias("o"),
        on="order_id",
        how="left"
    )

    # Get payment type per order
    payment_type = payments.select(
        "order_id", "payment_type"
    ).dropDuplicates(["order_id"])

    fact = fact.join(
        payment_type.alias("p"),
        on="order_id",
        how="left"
    )

    # Join reviews to get review_score
    review_score = reviews.groupBy("order_id").agg(
        spark_avg("review_score").alias("avg_review_score")
    )
    fact = fact.join(
        review_score.alias("r"),
        on="order_id",
        how="left"
    )

    # =========================
    # LOOKUP SURROGATE KEYS
    # =========================

    # Get customer_sk from dim_customer
    fact = fact.join(
        dim_customer.select("customer_id", "customer_sk"),
        on="customer_id",
        how="left"
    )

    # Get product_sk from dim_product
    fact = fact.join(
        dim_product.select("product_id", "product_sk"),
        on="product_id",
        how="left"
    )

    # Get seller_sk from dim_seller
    fact = fact.join(
        dim_seller.select("seller_id", "seller_sk"),
        on="seller_id",
        how="left"
    )

    # Get date_id from dim_date
    fact = fact.withColumn(
        "order_purchase_date",
        to_date(col("order_purchase_timestamp"))
    )
    fact = fact.join(
        dim_date.select("full_date", "date_id"),
        fact["order_purchase_date"] == dim_date["full_date"],
        how="left"
    )

    # =========================
    # COMPUTE DERIVED FACTS
    # =========================

    # Total amount per item
    fact = fact.withColumn(
        "total_amount",
        round(col("price") + col("freight_value"), 2)
    )

    # Delivery days
    fact = fact.withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    )

    # Is late delivery (1 = late, 0 = on time)
    fact = fact.withColumn(
        "is_late_delivery",
        when(
            col("order_delivered_customer_date") >
            col("order_estimated_delivery_date"),
            1
        ).otherwise(0)
    )

    # =========================
    # SELECT FINAL COLUMNS
    # =========================
    fact = fact.select(
        # Degenerate dimensions
        col("order_id"),
        col("order_item_id"),
        col("order_status"),

        # Foreign keys to dimensions
        col("customer_sk"),
        col("product_sk"),
        col("seller_sk"),
        col("date_id"),

        # Facts / measurements
        col("price"),
        col("freight_value"),
        col("total_amount"),
        col("delivery_days"),
        col("is_late_delivery"),
        col("avg_review_score"),
        col("payment_type"),

        # Partition columns
        col("order_purchase_date")
    )

    # Drop rows missing critical keys
    fact = fact.dropna(
        subset=["order_id", "customer_sk", "product_sk"]
    )

    return fact

# =========================
# MAIN
# =========================
def main():
    # Read all curated tables
    orders, customers, order_items, products, \
    sellers, payments, reviews = read_all_tables()

    # Build dimensions first
    dim_customer = build_dim_customer(customers)
    dim_product  = build_dim_product(products)
    dim_seller   = build_dim_seller(sellers)
    dim_date     = build_dim_date(orders)

    # Build fact table using dimension surrogate keys
    fact_order_items = build_fact_order_items(
        orders, order_items, customers, products,
        sellers, payments, reviews,
        dim_customer, dim_product, dim_seller, dim_date
    )

    # Write dimensions
    write_dimension(dim_customer,  "dim_customer")
    write_dimension(dim_product,   "dim_product")
    write_dimension(dim_seller,    "dim_seller")
    write_dimension(dim_date,      "dim_date")

    # Write fact table partitioned by date
    write_fact(
        fact_order_items,
        "fact_order_items",
        partition_cols=["order_purchase_date"]
    )

    logger.info("🎉 Star schema built successfully")

main()
job.commit()