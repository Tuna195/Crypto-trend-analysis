import time
import schedule
from logger import get_logger

# Import đúng hàm từ 2 module chiến binh
from twitter_client import fetch_and_produce as market_fetch
from whale_client import fetch_and_produce as whale_fetch

# Khởi tạo sổ nhật ký cho Nhạc trưởng
logger = get_logger("Scheduler")

def job_market_trend():
    logger.info(">>> KÍCH HOẠT: Bắt đầu luồng quét xu hướng thị trường (Market Trend)...")
    try:
        market_fetch()
    except Exception as e:
        logger.error(f"Lỗi khi chạy Market Trend: {e}")

def job_whale_tracking():
    logger.info(">>> KÍCH HOẠT: Bắt đầu luồng dò tìm Cá voi (Whale Tracking)...")
    try:
        whale_fetch()
    except Exception as e:
        logger.error(f"Lỗi khi chạy Whale Tracking: {e}")

def main():
    logger.info("==================================================")
    logger.info(" HỆ THỐNG INGESTION BẮT ĐẦU CHẠY TỰ ĐỘNG ")
    logger.info("==================================================")

    # Cấu hình lịch trình chính thức
    schedule.every(1).hours.do(job_market_trend)
    schedule.every(3).hours.do(job_whale_tracking)

    # Chạy ngay lần đầu khi khởi động
    job_market_trend()
    job_whale_tracking()

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info(" Đã nhận lệnh dừng từ người dùng. Tắt hệ thống Scheduler an toàn.")