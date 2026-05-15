import time
from collections import deque

from kafka_connection import get_kafka_producer
from api_helper import fetch_tweets_with_fallback
from logger import get_logger

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

    # 1. Tính toán mốc thời gian
    # Lấy dữ liệu trong vòng 60 phút vừa qua (3600 giây)
    hien_tai = int(time.time())
    thoi_gian_truoc = hien_tai - 3600 
    
    # 2. Xây dựng câu lệnh Advanced Search hoàn hảo (Đã mở rộng lên 8 đồng TOP Coin)
    cau_lenh = f"($BTC OR $ETH OR $SOL OR $XRP OR $ADA OR $BNB OR $DOGE OR $AVAX) -filter:replies lang:en since_time:{thoi_gian_truoc} until_time:{hien_tai}"
    
    # 3. Thông số API
    querystring = {"query": cau_lenh, "search_type": "Latest"}
    
    logger.info(f"\n--- Đang quét thị trường 8 ĐỒNG COIN (Pseudo-Streaming) ---")
    logger.info(f"Query: {cau_lenh}")
    total_count = 0
    
    try:
        # Sử dụng thuật toán Bắn tỉa dự phòng 5 Keys thay vì Requests.get chay
        data = fetch_tweets_with_fallback(querystring, logger)
        if not data:
            return # Dừng nếu toàn bộ 5 key đều tạch
        
        # API trả về danh sách bài viết nằm trong key 'timeline'
        tweets = data.get('timeline') or data.get('data') or data.get('results') or data.get('tweets', [])
        
        if not tweets or not isinstance(tweets, list):
            logger.warning("[CẢNH BÁO] Không có dữ liệu hoặc cấu trúc JSON thay đổi.")
            return
            
        for item in tweets:
            # Bỏ qua nếu không phải là tweet
            if item.get("type") != "tweet" and "tweet_id" not in item:
                continue

            # Lấy ID của bài viết
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