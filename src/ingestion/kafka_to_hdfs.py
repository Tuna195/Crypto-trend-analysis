import os
import sys
import json
import time
from collections import defaultdict
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Thêm đường dẫn thư mục 'src' vào sys.path để import các module khác (như storage)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '../../'))
src_dir = os.path.join(project_root, 'src')
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from storage.hdfs_client import HDFSStorageClient, HDFSConfig
from ingestion.logger import get_logger

# Nạp cấu hình từ file .env
load_dotenv(dotenv_path=os.path.join(project_root, '.env'), override=True)

logger = get_logger("Kafka_To_HDFS")

# ==========================================
# CẤU HÌNH THÔNG SỐ
# ==========================================
KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = os.getenv("KAFKA_PORT")

# Hỗ trợ lấy nhiều topic từ môi trường
topic_market = os.getenv("KAFKA_TOPIC_MARKET", "raw-tweets-market")
topic_whales = os.getenv("KAFKA_TOPIC_WHALES", "raw-tweets-whales")
TOPICS = [topic_market, topic_whales]

# Nơi chứa chứng chỉ kết nối Aiven Kafka
CERTS_DIR = os.getenv("KAFKA_CERTS_DIR", "")
if not CERTS_DIR.strip():
    CERTS_DIR = os.path.join(current_dir, 'certs')

BATCH_SIZE = 500       # Ghi xuống HDFS mỗi khi gom đủ 500 tweets
BATCH_INTERVAL = 60    # Hoặc ghi xuống HDFS mỗi 60 giây (tùy điều kiện nào đến trước)

def start_sink_process():
    # 1. KHỞI TẠO HDFS CLIENT
    logger.info("Đang kết nối tới cụm HDFS...")
    hdfs_config = HDFSConfig()
    hdfs_client = HDFSStorageClient(hdfs_config)
    if not hdfs_client.healthcheck():
        logger.error("[LỖI] Không thể kết nối tới HDFS. Vui lòng kiểm tra lại Docker/HDFS.")
        return
    logger.info("[OK] Đã kết nối HDFS thành công.")

    # 2. KHỞI TẠO KAFKA CONSUMER
    logger.info(f"Đang kết nối tới Aiven Kafka để lắng nghe các topic: {TOPICS}...")
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
            security_protocol="SSL",
            ssl_cafile=os.path.join(CERTS_DIR, "ca.pem"),
            ssl_certfile=os.path.join(CERTS_DIR, "service.cert"),
            ssl_keyfile=os.path.join(CERTS_DIR, "service.key"),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False, # Tự quản lý việc commit để tránh mất dữ liệu
            group_id='hdfs-sink-group',
            consumer_timeout_ms=1000 # Time-out để vòng lặp không bị kẹt vô hạn
        )
        logger.info("[OK] Đã kết nối tới Kafka thành công.")
    except Exception as e:
        logger.error(f"[LỖI] Kết nối Kafka thất bại: {e}")
        return

    # 3. VÒNG LẶP HÚT VÀ LƯU DỮ LIỆU
    buffer = []
    last_flush_time = time.time()

    logger.info(">>> Đang chờ dữ liệu chảy về để gom batch và đẩy xuống HDFS...")
    
    try:
        while True:
            # Dùng poll thay vì lặp trực tiếp trên consumer để dễ kiểm soát thời gian nghỉ
            msg_pack = consumer.poll(timeout_ms=2000)
            
            for tp, messages in msg_pack.items():
                for message in messages:
                    tweet = message.value
                    if tweet:
                        buffer.append(tweet)

            current_time = time.time()
            time_elapsed = current_time - last_flush_time
            
            # Kích hoạt flush khi đủ số lượng HOẶC đủ thời gian trễ
            if len(buffer) >= BATCH_SIZE or (len(buffer) > 0 and time_elapsed >= BATCH_INTERVAL):
                logger.info(f"Đã gom được {len(buffer)} bài viết sau {int(time_elapsed)}s. Bắt đầu đẩy xuống HDFS...")
                
                # Phân loại tweets theo `target_coin` (vì hàm store_raw_tweets lưu thư mục theo coin)
                grouped_tweets = defaultdict(list)
                for tweet in buffer:
                    # Mặc định gom nhóm 'MIXED' nếu không có target_coin
                    coin_group = tweet.get("target_coin", "MIXED")
                    grouped_tweets[coin_group].append(tweet)
                
                # Lưu từng nhóm xuống HDFS
                success = True
                for coin, tweets in grouped_tweets.items():
                    try:
                        file_path = hdfs_client.store_raw_tweets(coin, tweets)
                        logger.info(f"  -> Đã lưu {len(tweets)} bài của nhóm [{coin}] vào file: {file_path}")
                    except Exception as e:
                        logger.error(f"  -> [LỖI] Lỗi khi lưu nhóm [{coin}] xuống HDFS: {e}")
                        success = False
                
                # Nếu lưu thành công toàn bộ, ta commit Kafka offset (xác nhận đã đọc xong)
                if success:
                    consumer.commit()
                    logger.info("[OK] Đã commit offset lên Kafka. Dọn dẹp buffer.\n")
                else:
                    logger.warning("[CẢNH BÁO] Có lỗi khi lưu HDFS, offset chưa được commit đầy đủ.\n")
                
                # Reset bộ đệm
                buffer.clear()
                last_flush_time = current_time

    except KeyboardInterrupt:
        logger.info("\nĐã nhận lệnh ngắt từ người dùng (Ctrl+C). Đang tắt trạm thu an toàn...")
    finally:
        consumer.close()
        logger.info("Trạm thu Kafka To HDFS đã đóng.")

if __name__ == "__main__":
    start_sink_process()
