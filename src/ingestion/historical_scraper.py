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

logger = get_logger("Historical_Bot")

def fetch_historical():
    """
    Kịch bản này chạy 1 lần duy nhất để cào toàn bộ dữ liệu của 3 ngày vừa qua.
    Nó chia thời gian thành các khối (chunks) 4 giờ để không bị dính giới hạn API phân trang (Limit 20 bài).
    """
    producer = get_kafka_producer()
    if not producer: return
    
    # Cấu hình "Cỗ máy thời gian"
    days_to_fetch = 3        # Lùi về 3 ngày trước
    hours_per_chunk = 4      # Mỗi lần quét 1 khoảng 4 tiếng
    total_chunks = (days_to_fetch * 24) // hours_per_chunk
    
    now = int(time.time())
    chunk_seconds = hours_per_chunk * 3600
    
    base_query = "($BTC OR $ETH OR $SOL) -filter:replies lang:en"
    url = f"https://{RAPIDAPI_HOST}/search.php"
    headers = {"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST}
    
    total_saved = 0
    
    for i in range(total_chunks):
        until_time = now - (i * chunk_seconds)
        since_time = until_time - chunk_seconds
        
        full_query = f"{base_query} since_time:{since_time} until_time:{until_time}"
        querystring = {"query": full_query, "search_type": "Latest"}
        
        logger.info(f"Đang quét mốc quá khứ (Chunk {i+1}/{total_chunks}) | Query: {full_query}")
        
        try:
            response = requests.get(url, headers=headers, params=querystring)
            if response.status_code == 200:
                data = response.json()
                tweets = data.get('timeline') or data.get('data') or data.get('results') or data.get('tweets', [])
                
                count_in_chunk = 0
                for item in tweets:
                    if item.get("type") != "tweet" and "tweet_id" not in item: continue
                    tweet_id = str(item.get("tweet_id") or item.get("id_str") or item.get("id", ""))
                    
                    author = item.get("screen_name")
                    if not author and item.get("user_info"):
                        author = item.get("user_info").get("screen_name")
                    
                    clean_tweet = {
                        "id": tweet_id,
                        "text": item.get("text") or item.get("full_text", ""),
                        "created_at": item.get("created_at") or item.get("timestamp"),
                        "author": author or "unknown",
                        "target_coin": "MULTI_CRYPTO"
                    }
                    if clean_tweet["text"]:
                        producer.send("raw-tweets-market", key="HISTORICAL", value=clean_tweet)
                        count_in_chunk += 1
                        total_saved += 1
                        
                        # In log cho từng bài viết để bạn dễ theo dõi
                        short_text = clean_tweet['text'][:50].replace('\n', ' ')
                        logger.info(f"    -> Bơm dữ liệu [@{clean_tweet['author']}]: {short_text}...")
                logger.info(f" -> Thu được {count_in_chunk} bài viết lịch sử.")
            else:
                logger.error(f"Lỗi API: {response.status_code}")
        except Exception as e:
            logger.error(f"Lỗi hệ thống: {e}")
            
        # Rất quan trọng: Ngủ 3 giây để tránh bị RapidAPI ban vì gọi quá nhanh
        time.sleep(3) 
        
    producer.flush()
    logger.info(f"==== HOÀN TẤT: Đã cào được tổng cộng {total_saved} bài viết lịch sử của {days_to_fetch} ngày qua ====")

if __name__ == "__main__":
    fetch_historical()
