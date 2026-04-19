import time
import os
import requests
from dotenv import load_dotenv

from kafka_connection import get_kafka_producer
from logger import get_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '../../.env')
load_dotenv(dotenv_path=env_path, override=True)

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST")
KAFKA_TOPIC = "raw-tweets-market"

#khởi tạo logger
logger = get_logger("Market_Bot")

def fetch_and_produce():
    # Gọi cỗ máy bơm ra để dùng
    producer = get_kafka_producer()
    if not producer:
        return # Nếu lỗi kết nối thì dừng luôn

    target_coins = ["$BTC", "$ETH", "$SOL"]
    url = f"https://{RAPIDAPI_HOST}/api/search/latest"
    headers = {"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST}
    total_count = 0
    
    for coin in target_coins:
        logger.info(f"\n--- Đang quét thị trường cho: {coin} ---")
        querystring = {"keyword": coin, "count": "20"}
        
        try:
            response = requests.get(url, headers=headers, params=querystring)
            if response.status_code != 200:
                logger.error(f"[LỖI API] Trạng thái {response.status_code} với {coin}")
                continue
                
            data = response.json()
            tweets = data.get('data') or data.get('results') or data.get('timeline')
            
            if not tweets or not isinstance(tweets, list):
                continue
                
            for item in tweets:
                author_id = item.get("user_id_str")
                author_display = f"ID_{author_id}" if author_id else "unknown"

                clean_tweet = {
                    "id": item.get("id_str") or item.get("id"),
                    "text": item.get("full_text") or item.get("text", ""),
                    "created_at": item.get("created_at") or item.get("timestamp"),
                    "author": author_display,
                    "target_coin": coin
                }
                
                if clean_tweet["text"]:
                    producer.send(KAFKA_TOPIC, key=coin, value=clean_tweet)
                    short_text = clean_tweet['text'][:50].replace('\n', ' ')
                    logger.info(f"-> [{coin}] Bơm [{clean_tweet['author']}]: {short_text}...")
                    total_count += 1
                    
        except Exception as e:
            logger.info(f"[LỖI HỆ THỐNG] Lỗi khi xử lý {coin}: {e}")
            
        time.sleep(2)

    producer.flush()
    logger.info(f"\n[HOÀN TẤT] Đã bơm thành công {total_count} tweets từ 3 hệ sinh thái lên Kafka!")

if __name__ == "__main__":
    fetch_and_produce()