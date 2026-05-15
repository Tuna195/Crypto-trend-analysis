import time

from kafka_connection import get_kafka_producer
from api_helper import fetch_tweets_with_fallback
from logger import get_logger

logger = get_logger("Historical_Bot")

def fetch_historical():
    """
    Kịch bản này chạy 1 lần duy nhất để cào toàn bộ dữ liệu của 30 ngày vừa qua cho CẢ 2 luồng.
    Chunk được hạ xuống 1 giờ để vét cạn không sót bài nào. (Tổng chi phí khoảng 1440 request).
    """
    producer = get_kafka_producer()
    if not producer: return
    
    # Cấu hình "Cỗ máy thời gian"
    days_to_fetch = 30       # Lùi về 1 tháng (30 ngày) trước
    hours_per_chunk = 1      # Hạ xuống 1 tiếng/mẻ để vớt cạn dữ liệu
    total_chunks = (days_to_fetch * 24) // hours_per_chunk
    
    now = int(time.time())
    chunk_seconds = hours_per_chunk * 3600
    
    # Hai luồng dữ liệu cần vét lịch sử
    targets = [
        {
            "name": "MARKET_TREND",
            "topic": "raw-tweets-market",
            "query": "($BTC OR $ETH OR $SOL OR $XRP OR $ADA OR $BNB OR $DOGE OR $AVAX) -filter:replies lang:en",
            "key": "Crypto"
        },
        {
            "name": "WHALE_SIGNAL",
            "topic": "raw-tweets-whales",
            "query": "(from:elonmusk OR from:saylor OR from:VitalikButerin OR from:cz_binance OR from:brian_armstrong OR from:justinsuntron OR from:CryptoKaleo OR from:Pentosh1) -filter:replies",
            "key": "whale"
        }
    ]
    
    for target in targets:
        logger.info(f"\n=======================================================")
        logger.info(f"BẮT ĐẦU VÉT LỊCH SỬ CHO LUỒNG: {target['name']}")
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
