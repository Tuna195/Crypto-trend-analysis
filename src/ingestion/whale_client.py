import os
import time
import json
from collections import deque

from kafka_connection import get_kafka_producer
from api_helper import fetch_tweets_with_fallback
from logger import get_logger

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_WHALES", "raw-tweets-whales")

# Khởi tạo logger
logger = get_logger("Whale_Bot")

# Bộ nhớ tạm để khử trùng lặp
seen_tweets = deque(maxlen=2000)

def fetch_and_produce():
    producer = get_kafka_producer()
    if not producer:
        return 

    # Lấy dữ liệu trong vòng 3 giờ qua
    hien_tai = int(time.time())
    thoi_gian_truoc = hien_tai - 10800 
    
    # Gom các Cá Voi vào chung 1 câu lệnh Advanced Search (Đọc từ môi trường)
    whales_raw = os.getenv("TRACKED_WHALES")
    if whales_raw:
        whales = [w.strip() for w in whales_raw.split(",") if w.strip()]
    else:
        whales = ["elonmusk", "saylor", "VitalikButerin", "cz_binance", "brian_armstrong", "justinsuntron", "CryptoKaleo", "Pentosh1"]
        
    whales_query = " OR ".join([f"from:{w}" for w in whales])
    cau_lenh = f"({whales_query}) -filter:replies since_time:{thoi_gian_truoc} until_time:{hien_tai}"
    
    querystring = {"query": cau_lenh, "search_type": "Latest"}
    
    logger.info(f"\n--- Đang quét {len(whales)} MỤC TIÊU VIP (Pseudo-Streaming) ---")
    logger.info(f"Query: {cau_lenh}")
    total_count = 0
    
    try:
        # Sử dụng thuật toán Bắn tỉa dự phòng
        data = fetch_tweets_with_fallback(querystring, logger)
        if not data:
            return
            
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
                
                # Đóng gói dữ liệu chuẩn chỉ (Đã bổ sung các chỉ số tương tác và user metadata)
                user_info = item.get("user_info") or {}
                clean_tweet = {
                    "id": tweet_id,
                    "text": item.get("text") or item.get("full_text", ""),
                    "created_at": item.get("created_at") or item.get("timestamp"),
                    "author": author or "unknown",
                    "target_coin": "WHALE_SIGNAL",
                    "like_count": item.get("favorites") or 0,
                    "retweet_count": item.get("retweets") or 0,
                    "reply_count": item.get("replies") or 0,
                    "quote_count": item.get("quotes") or 0,
                    "bookmark_count": item.get("bookmarks") or 0,
                    "followers_count": user_info.get("followers_count") or 0,
                    "verified": user_info.get("verified") or False
                }
                
                if clean_tweet["text"]:
                    # In debug log JSON đầy đủ gửi lên Kafka để người dùng kiểm tra
                    logger.info(f"[DEBUG KAFKA JSON] {json.dumps(clean_tweet, ensure_ascii=False)}")
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