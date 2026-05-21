import os
import time

from kafka_connection import get_kafka_producer
from api_helper import fetch_tweets_with_fallback
from logger import get_logger

logger = get_logger("Historical_Bot")

def fetch_historical():
    """
    Kịch bản này chạy 1 lần duy nhất để cào toàn bộ dữ liệu lịch sử cho CẢ 2 luồng.
    Luồng Market Trend dùng chunk 1 giờ, luồng Whale Signal dùng chunk 24 giờ để tiết kiệm API keys.
    """
    producer = get_kafka_producer()
    if not producer: return
    
    # Đọc số ngày cần cào từ biến môi trường (mặc định là 30 ngày)
    days_raw = os.getenv("HISTORICAL_DAYS_TO_FETCH")
    try:
        days_to_fetch = int(days_raw) if days_raw else 30
    except ValueError:
        days_to_fetch = 30

    logger.info(f"Cấu hình Cỗ máy thời gian: Vét lịch sử trong {days_to_fetch} ngày qua.")
    now = int(time.time())
    
    # Cấu hình động dựa trên biến môi trường
    coins_raw = os.getenv("TRACKED_COINS")
    if coins_raw:
        coins = [c.strip() for c in coins_raw.split(",") if c.strip()]
    else:
        coins = ["BTC", "ETH", "SOL", "XRP", "ADA", "BNB", "DOGE", "AVAX"]
    coin_query = " OR ".join([f"${c}" for c in coins])
    market_query = f"({coin_query}) -filter:replies lang:en"

    whales_raw = os.getenv("TRACKED_WHALES")
    if whales_raw:
        whales = [w.strip() for w in whales_raw.split(",") if w.strip()]
    else:
        whales = ["elonmusk", "saylor", "VitalikButerin", "cz_binance", "brian_armstrong", "justinsuntron", "CryptoKaleo", "Pentosh1"]
    whales_query = " OR ".join([f"from:{w}" for w in whales])
    whale_query = f"({whales_query}) -filter:replies"

    # Hai luồng dữ liệu cần vét lịch sử (với chunk_size khác nhau để tối ưu request)
    targets = [
        {
            "name": "MARKET_TREND",
            "topic": os.getenv("KAFKA_TOPIC_MARKET", "raw-tweets-market"),
            "query": market_query,
            "key": "Crypto",
            "hours_per_chunk": 1
        },
        {
            "name": "WHALE_SIGNAL",
            "topic": os.getenv("KAFKA_TOPIC_WHALES", "raw-tweets-whales"),
            "query": whale_query,
            "key": "whale",
            "hours_per_chunk": 24
        }
    ]
    
    for target in targets:
        hours_per_chunk = target.get("hours_per_chunk", 1)
        total_chunks = (days_to_fetch * 24) // hours_per_chunk
        chunk_seconds = hours_per_chunk * 3600

        logger.info(f"\n=======================================================")
        logger.info(f"BẮT ĐẦU VÉT LỊCH SỬ CHO LUỒNG: {target['name']}")
        logger.info(f"Cấu hình: Chunk size = {hours_per_chunk}h | Tổng số chunks = {total_chunks}")
        logger.info(f"=======================================================\n")
        
        total_saved = 0
        for i in range(total_chunks):
            until_time = now - (i * chunk_seconds)
            since_time = until_time - chunk_seconds
            
            full_query = f"{target['query']} since_time:{since_time} until_time:{until_time}"
            querystring = {"query": full_query, "search_type": "Latest"}
            
            logger.info(f"[{target['name']}] Quét quá khứ (Chunk {i+1}/{total_chunks})")
            
            try:
                data = fetch_tweets_with_fallback(querystring, logger)
                if not data:
                    logger.error("Dừng Cỗ máy thời gian vì toàn bộ Key đã hết đạn!")
                    return # Thoát hẳn chương trình
                    
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
                        "target_coin": target['name']
                    }
                    if clean_tweet["text"]:
                        producer.send(target['topic'], key=target['key'], value=clean_tweet)
                        count_in_chunk += 1
                        total_saved += 1
                        
                        short_text = clean_tweet['text'][:50].replace('\n', ' ')
                        logger.info(f"    -> [LỊCH SỬ {target['name']}] Bơm: @{clean_tweet['author']}: {short_text}...")
                
            except Exception as e:
                logger.error(f"Lỗi hệ thống: {e}")
                
            # Nghỉ ngơi giữa các chunk
            time.sleep(2) 
            
        logger.info(f"==== HOÀN TẤT VÉT CẠN LUỒNG {target['name']}: THU ĐƯỢC {total_saved} BÀI ====\n")

    producer.flush()
    logger.info("🎉 TOÀN BỘ CHIẾN DỊCH VÉT LỊCH SỬ ĐÃ THÀNH CÔNG RỰC RỠ!")

if __name__ == "__main__":
    fetch_historical()
