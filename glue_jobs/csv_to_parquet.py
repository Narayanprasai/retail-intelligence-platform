import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, lower, upper, trim, regexp_replace,
    to_timestamp, year, month
)
from pyspark.sql.types import DoubleType, IntegerType
import logging

# =========================
# SETUP
# =========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'raw_bucket',
    'curated_bucket',
    'database_name'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_BUCKET = args['raw_bucket']
CURATED_BUCKET = args['curated_bucket']

logger.info(f"Starting csv_to_parquet job")
logger.info(f"Raw bucket: {RAW_BUCKET}")
logger.info(f"Curated bucket: {CURATED_BUCKET}")

# =========================
# HELPER FUNCTIONS
# =========================
def read_from_s3(spark, path):
    """Read CSV directly from S3"""
    logger.info(f"Reading from {path}")
    return spark.read \
        .option("header", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("multiLine", "true") \
        .csv(path)

def write_parquet(df, path, partition_cols=None):
    """Write DataFrame as partitioned Parquet to S3"""
    logger.info(f"Writing to {path}")
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(partition_cols)
    writer.save(path)
    logger.info(f"✅ Written successfully")

def clean_string_col(df, col_name):
    """Strip quotes and whitespace from string column"""
    return df.withColumn(
        col_name,
        trim(regexp_replace(col(col_name), '"', ''))
    )

# =========================
# TRANSFORM ORDERS
# =========================
def transform_orders(df):
    logger.info("Transforming orders...")

    for c in df.columns:
        df = clean_string_col(df, c)

    df = df.withColumn(
        "order_purchase_timestamp",
        to_timestamp(col("order_purchase_timestamp"))
    )
    df = df.withColumn(
        "order_approved_at",
        to_timestamp(col("order_approved_at"))
    )
    df = df.withColumn(
        "order_delivered_carrier_date",
        to_timestamp(col("order_delivered_carrier_date"))
    )
    df = df.withColumn(
        "order_delivered_customer_date",
        to_timestamp(col("order_delivered_customer_date"))
    )
    df = df.withColumn(
        "order_estimated_delivery_date",
        to_timestamp(col("order_estimated_delivery_date"))
    )
    df = df.withColumn("year",
        year(col("order_purchase_timestamp")))
    df = df.withColumn("month",
        month(col("order_purchase_timestamp")))
    df = df.withColumn("order_status",
        lower(trim(col("order_status"))))
    df = df.dropna(subset=["order_id", "customer_id"])

    logger.info(f"Orders transformed: {df.count()} rows")
    return df

# =========================
# TRANSFORM CUSTOMERS
# =========================
def transform_customers(df):
    logger.info("Transforming customers...")

    for c in df.columns:
        df = clean_string_col(df, c)

    df = df.withColumn("customer_city",
        lower(trim(col("customer_city"))))
    df = df.withColumn("customer_state",
        upper(trim(col("customer_state"))))
    df = df.dropna(subset=["customer_id"])
    df = df.dropDuplicates(["customer_id"])

    logger.info(f"Customers transformed: {df.count()} rows")
    return df

# =========================
# TRANSFORM ORDER ITEMS
# =========================
def transform_order_items(df):
    logger.info("Transforming order items...")

    for c in df.columns:
        df = clean_string_col(df, c)

    df = df.withColumn("price",
        col("price").cast(DoubleType()))
    df = df.withColumn("freight_value",
        col("freight_value").cast(DoubleType()))
    df = df.withColumn("order_item_id",
        col("order_item_id").cast(IntegerType()))
    df = df.dropna(subset=["order_id", "product_id"])

    logger.info(f"Order items transformed: {df.count()} rows")
    return df

# =========================
# TRANSFORM PRODUCTS
# =========================
def transform_products(df):
    logger.info("Transforming products...")

    for c in df.columns:
        df = clean_string_col(df, c)

    numeric_cols = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]
    for nc in numeric_cols:
        df = df.withColumn(nc, col(nc).cast(DoubleType()))

    df = df.withColumn("product_category_name",
        lower(trim(col("product_category_name"))))
    df = df.dropna(subset=["product_id"])
    df = df.dropDuplicates(["product_id"])

    logger.info(f"Products transformed: {df.count()} rows")
    return df

# =========================
# TRANSFORM PAYMENTS
# =========================
def transform_payments(df):
    logger.info("Transforming payments...")

    for c in df.columns:
        df = clean_string_col(df, c)

    df = df.withColumn("payment_value",
        col("payment_value").cast(DoubleType()))
    df = df.withColumn("payment_installments",
        col("payment_installments").cast(IntegerType()))
    df = df.withColumn("payment_sequential",
        col("payment_sequential").cast(IntegerType()))
    df = df.dropna(subset=["order_id"])

    logger.info(f"Payments transformed: {df.count()} rows")
    return df

# =========================
# TRANSFORM REVIEWS
# =========================
def transform_reviews(df):
    logger.info("Transforming reviews...")

    for c in df.columns:
        df = clean_string_col(df, c)

    df = df.withColumn("review_score",
        col("review_score").cast(IntegerType()))
    df = df.withColumn("review_creation_date",
        to_timestamp(col("review_creation_date")))
    df = df.withColumn("review_answer_timestamp",
        to_timestamp(col("review_answer_timestamp")))
    df = df.dropna(subset=["review_id", "order_id"])

    logger.info(f"Reviews transformed: {df.count()} rows")
    return df

# =========================
# TRANSFORM SELLERS
# =========================
def transform_sellers(df):
    logger.info("Transforming sellers...")

    for c in df.columns:
        df = clean_string_col(df, c)

    df = df.withColumn("seller_city",
        lower(trim(col("seller_city"))))
    df = df.dropna(subset=["seller_id"])
    df = df.dropDuplicates(["seller_id"])

    logger.info(f"Sellers transformed: {df.count()} rows")
    return df

# =========================
# MAIN
# =========================
def main():
    tables = {
        "orders":      (transform_orders,      ["year", "month"]),
        "customers":   (transform_customers,   None),
        "order_items": (transform_order_items, None),
        "products":    (transform_products,    None),
        "payments":    (transform_payments,    None),
        "reviews":     (transform_reviews,     None),
        "sellers":     (transform_sellers,     None),
    }

    s3_paths = {
        "orders":      f"s3://{RAW_BUCKET}/olist/orders/",
        "customers":   f"s3://{RAW_BUCKET}/olist/customers/",
        "order_items": f"s3://{RAW_BUCKET}/olist/order_items/",
        "products":    f"s3://{RAW_BUCKET}/olist/products/",
        "payments":    f"s3://{RAW_BUCKET}/olist/payments/",
        "reviews":     f"s3://{RAW_BUCKET}/olist/reviews/",
        "sellers":     f"s3://{RAW_BUCKET}/olist/sellers/",
    }

    for table_name, (transform_fn, partition_cols) in tables.items():
        try:
            df = read_from_s3(spark, s3_paths[table_name])
            df_clean = transform_fn(df)
            output_path = f"s3://{CURATED_BUCKET}/{table_name}/"
            write_parquet(df_clean, output_path, partition_cols)
            logger.info(f"✅ {table_name} complete")

        except Exception as e:
            logger.error(f"❌ {table_name} failed: {str(e)}")
            raise e

    logger.info("🎉 All tables processed successfully")

main()
job.commit()