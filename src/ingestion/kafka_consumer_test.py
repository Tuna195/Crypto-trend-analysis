import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer

# ==========================================
# 1. NẠP CẤU HÌNH (Giống hệt file Producer)
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '../../.env')
load_dotenv(dotenv_path=env_path, override=True)

KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = os.getenv("KAFKA_PORT")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
CERTS_DIR = os.path.join(current_dir, 'certs')

# ==========================================
# 2. KHỞI TẠO TRẠM THU (CONSUMER)
# ==========================================
print(f"Đang kết nối tới topic '{KAFKA_TOPIC}' trên Aiven Kafka...")

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        security_protocol="SSL",
        ssl_cafile=os.path.join(CERTS_DIR, "ca.pem"),
        ssl_certfile=os.path.join(CERTS_DIR, "service.cert"),
        ssl_keyfile=os.path.join(CERTS_DIR, "service.key"),
        
        # Tự động dịch ngược dữ liệu nhị phân về lại dạng JSON
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        
        # CỰC KỲ QUAN TRỌNG: Lệnh này bảo Kafka "Hãy đọc lại từ bài đầu tiên trong quá khứ"
        auto_offset_reset='earliest',
        
        # Gom nhóm người đọc (Không bắt buộc nhưng nên có)
        group_id='test-reader-group' 
    )
    print("[OK] Kết nối thành công! Đang chờ dữ liệu chảy về...\n")
    print("-" * 50)
    
except Exception as e:
    print(f"[LỖI] Không thể kết nối Kafka: {e}")
    exit()

# ==========================================
# 3. LẮNG NGHE VÀ IN RA MÀN HÌNH
# ==========================================
try:
    count = 0
    # Vòng lặp này sẽ chạy vô tận để trực chờ dữ liệu mới
    for message in consumer:
        tweet = message.value
        author = tweet.get('author', 'unknown')
        text = tweet.get('text', '').replace('\n', ' ')
        
        print(f"[{count + 1}] 👤 {author}: {text[:80]}...")
        count += 1
        
except KeyboardInterrupt:
    print("\nĐã tắt trạm thu dữ liệu.")