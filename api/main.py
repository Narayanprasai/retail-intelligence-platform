from fastapi import FastAPI, HTTPException
from mangum import Mangum
import redshift_connector
import os
from dotenv import load_dotenv
from typing import Optional
import logging

# =========================
# SETUP
# =========================
load_dotenv()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Retail Intelligence API",
    description="Serves aggregated retail analytics from Redshift",
    version="1.0.0"
)

# =========================
# DATABASE CONNECTION
# =========================
def get_connection():
    """Get Redshift connection"""
    try:
        conn = redshift_connector.connect(
            host=os.getenv("REDSHIFT_HOST"),
            port=int(os.getenv("REDSHIFT_PORT", 5439)),
            database=os.getenv("REDSHIFT_DB"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD"),
            is_serverless=True,
            serverless_work_group="retail-platform-workgroup"
        )
        return conn
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

def execute_query(query: str) -> list:
    """Execute query and return results as list of dicts"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()

# =========================
# ENDPOINTS
# =========================

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "retail-intelligence-api",
        "version": "1.0.0"
    }

@app.get("/revenue/monthly")
def get_monthly_revenue(year: Optional[int] = None):
    """Total revenue by month"""
    year_filter = f"WHERE d.year = {year}" if year else ""
    query = f"""
        SELECT
            d.year,
            d.month,
            d.month_name,
            COUNT(DISTINCT f.order_id)    AS total_orders,
            ROUND(SUM(f.total_amount), 2) AS revenue,
            ROUND(AVG(f.total_amount), 2) AS avg_order_value
        FROM marts.fct_order_items f
        JOIN marts.dim_date d ON f.date_id = d.date_id
        {year_filter}
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
    """
    return {"data": execute_query(query)}

@app.get("/revenue/by-state")
def get_revenue_by_state(year: Optional[int] = None):
    """Total revenue by customer state"""
    year_filter = "AND d.year = {year}" if year else ""
    query = f"""
        SELECT
            c.customer_state,
            COUNT(DISTINCT f.order_id)    AS total_orders,
            ROUND(SUM(f.total_amount), 2) AS revenue,
            ROUND(AVG(f.delivery_days), 1) AS avg_delivery_days
        FROM marts.fct_order_items f
        JOIN marts.dim_customer c ON f.customer_sk = c.customer_sk
        JOIN marts.dim_date d ON f.date_id = d.date_id
        WHERE c.is_current = true
        {year_filter}
        GROUP BY c.customer_state
        ORDER BY revenue DESC
    """
    return {"data": execute_query(query)}

@app.get("/products/top10")
def get_top_products(year: Optional[int] = None):
    """Top 10 product categories by revenue"""
    year_filter = f"AND d.year = {year}" if year else ""
    query = f"""
        SELECT
            p.product_category_name,
            COUNT(DISTINCT f.order_id)    AS total_orders,
            ROUND(SUM(f.total_amount), 2) AS revenue,
            ROUND(AVG(f.avg_review_score), 2) AS avg_review_score
        FROM marts.fct_order_items f
        JOIN marts.dim_product p ON f.product_sk = p.product_sk
        JOIN marts.dim_date d ON f.date_id = d.date_id
        WHERE p.product_category_name IS NOT NULL
        {year_filter}
        GROUP BY p.product_category_name
        ORDER BY revenue DESC
        LIMIT 10
    """
    return {"data": execute_query(query)}

@app.get("/customers/summary")
def get_customer_summary():
    """Customer summary statistics"""
    query = """
        SELECT
            c.customer_state,
            COUNT(DISTINCT c.customer_id)  AS total_customers,
            COUNT(DISTINCT f.order_id)     AS total_orders,
            ROUND(SUM(f.total_amount), 2)  AS total_revenue,
            ROUND(AVG(f.total_amount), 2)  AS avg_order_value
        FROM marts.fct_order_items f
        JOIN marts.dim_customer c ON f.customer_sk = c.customer_sk
        WHERE c.is_current = true
        GROUP BY c.customer_state
        ORDER BY total_revenue DESC
        LIMIT 10
    """
    return {"data": execute_query(query)}

@app.get("/orders/late-delivery-rate")
def get_late_delivery_rate():
    """Late delivery rate by seller state"""
    query = """
        SELECT
            s.seller_state,
            COUNT(*)                          AS total_orders,
            SUM(f.is_late_delivery)           AS late_orders,
            ROUND(
                SUM(f.is_late_delivery) * 100.0 / COUNT(*),
                2
            )                                 AS late_delivery_pct,
            ROUND(AVG(f.delivery_days), 1)    AS avg_delivery_days
        FROM marts.fct_order_items f
        JOIN marts.dim_seller s ON f.seller_sk = s.seller_sk
        GROUP BY s.seller_state
        ORDER BY late_delivery_pct DESC
        LIMIT 10
    """
    return {"data": execute_query(query)}

# =========================
# LAMBDA HANDLER
# =========================
handler = Mangum(app)