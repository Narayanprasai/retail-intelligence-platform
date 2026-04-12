import boto3
import yaml
import os
import json
import logging
from datetime import datetime

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =========================
# LOAD CONFIG
# =========================
def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

# =========================
# UPLOAD TO S3
# =========================
def upload_to_s3(
    s3_client,
    local_path: str,
    bucket: str,
    s3_key: str
) -> bool:
    try:
        s3_client.upload_file(local_path, bucket, s3_key)
        logger.info(f"✅ Uploaded: s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed: {local_path} → {str(e)}")
        return False

# =========================
# DEAD LETTER QUEUE
# =========================
def write_to_dlq(dlq_log: str, record: dict) -> None:
    with open(dlq_log, 'a') as f:
        f.write(json.dumps(record) + '\n')
    logger.warning(f"⚠️  Written to DLQ: {record['source']}")

# =========================
# MAIN INGESTION
# =========================
def run_ingestion(config_path: str) -> None:
    config = load_config(config_path)
    
    # Setup
    s3_client = boto3.client('s3', region_name=config['region'])
    bucket = config['s3']['raw_bucket']
    prefix = config['s3']['prefix']
    dlq_log = config['dlq']['log_file']
    ingested_date = datetime.today().strftime('%Y-%m-%d')
    
    # Tracking
    total = len(config['sources'])
    succeeded = 0
    failed = 0
    
    logger.info(f" Starting ingestion — {total} sources")
    logger.info(f"Ingestion date: {ingested_date}")
    logger.info(f"Target bucket: {bucket}")
    
    # Process each source
    for source in config['sources']:
        name = source['name']
        local_path = source['local_path']
        filename = source['filename']
        critical = source['critical']
        
        # Build S3 key with partition
        s3_key = f"{prefix}/{name}/ingested_date={ingested_date}/{filename}"
        
        # Check file exists locally
        if not os.path.exists(local_path):
            logger.error(f"❌ File not found: {local_path}")
            record = {
                "source": name,
                "local_path": local_path,
                "error": "File not found",
                "timestamp": datetime.utcnow().isoformat(),
                "critical": critical
            }
            write_to_dlq(dlq_log, record)
            failed += 1

            # Stop pipeline if critical source missing
            if critical:
                logger.error(f"Critical source {name} missing — stopping pipeline")
                raise FileNotFoundError(f"Critical source missing: {name}")
            continue

        # Upload to S3
        success = upload_to_s3(s3_client, local_path, bucket, s3_key)

        if success:
            succeeded += 1
        else:
            failed += 1
            record = {
                "source": name,
                "local_path": local_path,
                "s3_key": s3_key,
                "error": "Upload failed",
                "timestamp": datetime.utcnow().isoformat(),
                "critical": critical
            }
            write_to_dlq(dlq_log, record)

            if critical:
                logger.error(f"Critical source {name} failed — stopping pipeline")
                raise Exception(f"Critical upload failed: {name}")

    # Summary
    logger.info("=" * 50)
    logger.info(f"✅ Succeeded: {succeeded}/{total}")
    logger.info(f"❌ Failed:    {failed}/{total}")
    if failed > 0:
        logger.info(f"⚠️  DLQ log:  {dlq_log}")
    logger.info("=" * 50)

# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    run_ingestion("pipeline_config.yaml")