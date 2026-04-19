import json
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from logger import get_logger

# ==========================================
# KHỞI TẠO BẢO MẬT CHUNG CHO TOÀN HỆ THỐNG
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '../../.env')
load_dotenv(dotenv_path=env_path, override=True)

KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = os.getenv("KAFKA_PORT")
CERTS_DIR = os.path.join(current_dir, 'certs')

#khởi tạo logger
logger = get_logger("Kafka_Base")

def get_kafka_producer():
    """
    Hàm này khởi tạo và trả về cỗ máy bơm Kafka đã cấu hình sẵn.
    """
    logger.info("Đang khởi động cỗ máy Kafka Cloud...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
            security_protocol="SSL",
            ssl_cafile=os.path.join(CERTS_DIR, "ca.pem"),
            ssl_certfile=os.path.join(CERTS_DIR, "service.cert"),
            ssl_keyfile=os.path.join(CERTS_DIR, "service.key"),
            acks='all',
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Đã kết nối thành công tới đường ống Aiven Kafka!")
        return producer
    except Exception as e:
        logger.error(f"Không thể kết nối Kafka: {e}")
        return None