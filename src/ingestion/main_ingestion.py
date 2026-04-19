import time
import schedule
from logger import get_logger


from twitter_client import fetch_and_produce
from whale_client import track_whales 

# Khởi tạo sổ nhật ký cho Nhạc trưởng
logger = get_logger("Scheduler")

def job_market_trend():
    logger.info(">>> KÍCH HOẠT: Bắt đầu luồng quét xu hướng thị trường (Market Trend)...")
    try:
        fetch_and_produce()
    except Exception as e:
        logger.error(f"Lỗi khi chạy Market Trend: {e}")

def job_whale_tracking():
    logger.info(">>> KÍCH HOẠT: Bắt đầu luồng dò tìm Cá voi (Whale Tracking)...")
    try:
        track_whales()
    except Exception as e:
        logger.error(f"Lỗi khi chạy Whale Tracking: {e}")

def main():
    logger.info("==================================================")
    logger.info(" HỆ THỐNG INGESTION BẮT ĐẦU CHẠY TỰ ĐỘNG ")
    logger.info("==================================================")

    # 1. Cấu hình lịch trình
    # schedule.every(15).minutes.do(job_market_trend)    
    # schedule.every(1).hours.do(job_whale_tracking)

	# Test
    schedule.every(1).minutes.do(job_market_trend)
    schedule.every(2).minutes.do(job_whale_tracking)

	# Chạy lần đầu
    job_market_trend()
    job_whale_tracking()

    while True:
        # Kiểm tra lịch chạy
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Đã nhận lệnh dừng từ người dùng. Tắt hệ thống Scheduler an toàn.")