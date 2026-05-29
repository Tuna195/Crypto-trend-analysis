import os
import sys
import argparse
import subprocess
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.storage.mongo_client import MongoConfig, MongoStorageClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("batch_runner")


def fetch_last_processed_datetime(mongo: MongoStorageClient) -> datetime | None:
    try:
        cursor = (
            mongo.db.batch_sentiment_metrics
            .find({"window_end": {"$exists": True, "$ne": None}}, {"_id": 0, "window_end": 1})
            .sort("window_end", -1)
            .limit(1)
        )
        result = next(cursor, None)
        if result and result.get("window_end"):
            return result["window_end"]
    except Exception as exc:
        log.warning("Cannot query MongoDB: %s", exc)
    return None


def run_batch_job(target_date: str):
    cmd = [sys.executable, "src/processing/batch_layer/batch_job.py", "--target-date", target_date]
        
    log.info("=" * 60)
    log.info(f"STARTING SPARK FOR: {target_date} ALL_HOURS")
    log.info(f"Command: {' '.join(cmd)}")
    log.info("=" * 60)
    
    # Wait for process to finish
    result = subprocess.run(cmd, cwd=str(_PROJECT_ROOT))
    if result.returncode != 0:
        log.error(f"Spark Job failed for {target_date} (Exit code: {result.returncode})")
        return False
    
    log.info(f"Successfully processed {target_date}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Run Batch Job sequentially to avoid OutOfMemory errors")
    parser.add_argument("--start-date", type=str, help="Force scan from a specific date (YYYY-MM-DD) instead of auto-detecting")
    args = parser.parse_args()

    mongo_cfg = MongoConfig(
        uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"), 
        database=os.getenv("MONGO_DB", "crypto_trends")
    )
    mongo = MongoStorageClient(mongo_cfg)

    # 1. Determine start timestamp
    if args.start_date:
        last_dt = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) - timedelta(days=1)
        log.info(f"Manual mode: Force scanning from {args.start_date}")
    else:
        last_dt = fetch_last_processed_datetime(mongo)
        if not last_dt:
            log.info("No processing history found. Running full-scan (chunked by day from 7 days ago).")
            last_dt = datetime.now(timezone.utc) - timedelta(days=7)
        else:
            log.info(f"Auto mode: Last processed datetime in DB is {last_dt}")

    mongo.close()

    # 2. Build list of dates to run
    now = datetime.now(timezone.utc)
    current_date = last_dt.date()
    end_date = now.date()

    if current_date > end_date:
        log.info("No new data to process.")
        return

    tasks = []

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        tasks.append(date_str)
        current_date += timedelta(days=1)

    if not tasks:
        log.info("No new tasks to run.")
        return

    log.info(f"Scheduled {len(tasks)} Spark processes to run sequentially.")
    
    for date_str in tasks:
        success = run_batch_job(date_str)
        if not success:
            log.error("Halting the entire pipeline due to a process failure!")
            sys.exit(1)

    log.info("ALL PROCESSES COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    main()
