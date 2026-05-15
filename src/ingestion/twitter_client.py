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
from collections import deque

KAFKA_TOPIC = "raw-tweets-market"

# Khởi tạo logger
logger = get_logger("Market_Bot")

# Bộ nhớ tạm để khử trùng lặp (chứa 2000 tweet)
seen_tweets = deque(maxlen=2000)

def fetch_and_produce():
    # Gọi cỗ máy bơm ra để dùng
    producer = get_kafka_producer()
    if not producer:
        return 

    # 1. Tính toán mốc thời gian (UNIX Timestamp)
    # Lấy dữ liệu trong vòng 60 phút vừa qua (3600 giây)
    hien_tai = int(time.time())
    thoi_gian_truoc = hien_tai - 3600 
    
    # 2. Xây dựng câu lệnh Advanced Search hoàn hảo
    # Gom 3 đồng coin, loại bỏ reply, chỉ lấy tiếng Anh, lọc theo mốc thời gian
    cau_lenh = f"($BTC OR $ETH OR $SOL) -filter:replies lang:en since_time:{thoi_gian_truoc} until_time:{hien_tai}"
    
    # 3. Thông số API mới (Tùy thuộc API bạn chọn trên RapidAPI, sửa lại cho đúng)
    url = f"https://{RAPIDAPI_HOST}/search.php" # Hoặc /twitter/search
    headers = {"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST}
    querystring = {"query": cau_lenh, "search_type": "Latest"}
    
    logger.info(f"\n--- Đang quét thị trường (Pseudo-Streaming) ---")
    logger.info(f"Query: {cau_lenh}")
    total_count = 0
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code != 200:
            logger.error(f"[LỖI API] Trạng thái {response.status_code}: {response.text}")
            return
            
        data = response.json()
        
        # API trả về danh sách bài viết nằm trong key 'timeline' (theo test_request.json)
        tweets = data.get('timeline') or data.get('data') or data.get('results') or data.get('tweets', [])
        
        if not tweets or not isinstance(tweets, list):
            logger.warning("[CẢNH BÁO] Không có dữ liệu hoặc cấu trúc JSON thay đổi.")
            return
            
        for item in tweets:
            # Bỏ qua nếu không phải là tweet
            if item.get("type") != "tweet" and "tweet_id" not in item:
                continue

            # Lấy ID của bài viết (Trong JSON mẫu là "tweet_id")
            tweet_id = str(item.get("tweet_id") or item.get("id_str") or item.get("id", ""))
            
            # Khử trùng lặp
            if tweet_id not in seen_tweets:
                # Lấy tên tác giả từ trường screen_name hoặc user_info
                author = item.get("screen_name")
                if not author and item.get("user_info"):
                    author = item.get("user_info").get("screen_name")
                
                # Đóng gói dữ liệu chuẩn chỉ
                clean_tweet = {
                    "id": tweet_id,
                    "text": item.get("text") or item.get("full_text", ""),
                    "created_at": item.get("created_at") or item.get("timestamp"),
                    "author": author or "unknown",
                    "target_coin": "MULTI_CRYPTO"
                }
                
                if clean_tweet["text"]:
                    producer.send(KAFKA_TOPIC, key="CRYPTO", value=clean_tweet)
                    seen_tweets.append(tweet_id)
                    total_count += 1
                    
                    short_text = clean_tweet['text'][:50].replace('\n', ' ')
                    logger.info(f"-> Bơm dữ liệu [@{clean_tweet['author']}]: {short_text}...")
                    
    except Exception as e:
        logger.info(f"[LỖI HỆ THỐNG] Lỗi khi xử lý: {e}")
        
    producer.flush()
    logger.info(f"\n[HOÀN TẤT] Bơm thành công {total_count} tweets MỚI TINH lên Kafka!")

if __name__ == "__main__":
    fetch_and_produce()