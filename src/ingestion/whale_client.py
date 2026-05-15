import time
import os
import requests
from dotenv import load_dotenv
from collections import deque

from kafka_connection import get_kafka_producer
from logger import get_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '../../.env')
load_dotenv(dotenv_path=env_path, override=True)

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST")

KAFKA_TOPIC = "raw-tweets-whales"

# Khởi tạo logger
logger = get_logger("Whale_Bot")

# Bộ nhớ tạm để khử trùng lặp
seen_tweets = deque(maxlen=2000)

def fetch_and_produce():
    producer = get_kafka_producer()
    if not producer:
        return 

    # Lấy dữ liệu trong vòng 3 giờ qua (10800 giây)
    hien_tai = int(time.time())
    thoi_gian_truoc = hien_tai - 43200 
    
    # Gom các cá voi vào chung 1 câu lệnh Advanced Search
    cau_lenh = f"(from:elonmusk OR from:saylor OR from:VitalikButerin) -filter:replies since_time:{thoi_gian_truoc} until_time:{hien_tai}"
    
    url = f"https://{RAPIDAPI_HOST}/search.php"
    headers = {"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST}
    querystring = {"query": cau_lenh, "search_type": "Latest"}
    
    logger.info(f"\n--- Đang quét mục tiêu VIP (Pseudo-Streaming) ---")
    logger.info(f"Query: {cau_lenh}")
    total_count = 0
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code != 200:
            logger.error(f"[LỖI API] Trạng thái {response.status_code}: {response.text}")
            return
            
        data = response.json()
        tweets = data.get('timeline') or data.get('data') or data.get('results') or data.get('tweets', [])
        
        if not tweets or not isinstance(tweets, list):
            logger.warning("[CẢNH BÁO] Không có dữ liệu hoặc cấu trúc JSON thay đổi.")
            return
            
        for item in tweets:
            if item.get("type") != "tweet" and "tweet_id" not in item:
                continue

            tweet_id = str(item.get("tweet_id") or item.get("id_str") or item.get("id", ""))
            
            if tweet_id not in seen_tweets:
                author = item.get("screen_name")
                if not author and item.get("user_info"):
                    author = item.get("user_info").get("screen_name")
                
                clean_tweet = {
                    "id": tweet_id,
                    "text": item.get("text") or item.get("full_text", ""),
                    "created_at": item.get("created_at") or item.get("timestamp"),
                    "author": author or "unknown",
                    "target_coin": "WHALE_SIGNAL"
                }
                
                if clean_tweet["text"]:
                    producer.send(KAFKA_TOPIC, key="VIP", value=clean_tweet)
                    seen_tweets.append(tweet_id)
                    total_count += 1
                    
                    short_text = clean_tweet['text'][:50].replace('\n', ' ')
                    logger.info(f"-> Bơm dữ liệu [@{clean_tweet['author']}]: {short_text}...")
                    
    except Exception as e:
        logger.info(f"[LỖI HỆ THỐNG] Lỗi khi xử lý: {e}")
        
    producer.flush()
    logger.info(f"\n[HOÀN TẤT] Bơm thành công {total_count} tín hiệu CÁ VOI MỚI TINH lên Kafka!")

if __name__ == "__main__":
    fetch_and_produce()