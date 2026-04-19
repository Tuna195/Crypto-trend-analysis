import json
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
KAFKA_TOPIC = "raw-tweets-whales"
CACHE_FILE = os.path.join(current_dir, 'whales_cache.json')

#Khởi tạo logger
logger = get_logger("Whale_Bot")

# ==========================================
# HÀM lấy id của whale
# ==========================================
def get_whale_directory(target_names):
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            directory = json.load(f)
    else:
        directory = {}

    headers = {"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST}
    url_get_id = f"https://{RAPIDAPI_HOST}/api/user/get-user-id" 
    
    is_updated = False
    active_whales = {}

    for name in target_names:
        if name in directory:
            logger.info(f"[CACHE] Đã có ID của @{name} trong danh bạ nội bộ.")
            active_whales[name] = directory[name]
        else:
            logger.info(f"[API] Chưa có ID của @{name}, đang tra cứu lên RapidAPI...")
            querystring = {"username": name} 
            
            try:
                response = requests.get(url_get_id, headers=headers, params=querystring)
                if response.status_code == 200:
                    data = response.json()
                    user_id = data.get("data", {}).get("id")
                    
                    if user_id:
                        directory[name] = str(user_id)
                        active_whales[name] = str(user_id)
                        is_updated = True
                        logger.info(f"   -> [THÀNH CÔNG] Đã lưu ID: {user_id}")
                    else:
                        logger.error(f"   -> [LỖI TÌM KIẾM] Không thấy ID. Data: {data}")
                else:
                    logger.error(f"   -> [LỖI API] Trạng thái {response.status_code}")
                time.sleep(2) 
            except Exception as e:
                logger.error(f"   -> [LỖI HỆ THỐNG] {e}")

    if is_updated:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(directory, f, indent=4)
            logger.info("[HỆ THỐNG] Đã cập nhật file whales_cache.json!")

    return active_whales

# ==========================================
# LUỒNG QUÉT TWEET CÁ VOI
# ==========================================
def track_whales():
    target_list = ["elonmusk", "saylor", "VitalikButerin", "whale_alert"]
    whales_dict = get_whale_directory(target_list)
    
    if not whales_dict:
        logger.info("[THÔNG BÁO] Không có ID hợp lệ nào để theo dõi. Dừng chương trình.")
        return

    # Khởi tạo Kafka chỉ với 1 dòng lệnh duy nhất!
    producer = get_kafka_producer()
    if not producer:
        return

    url_get_tweets = f"https://{RAPIDAPI_HOST}/api/user/tweets"
    headers = {"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST}
    total_count = 0
    
    for name, user_id in whales_dict.items():
        logger.info(f"\n--- Đang quét radar mục tiêu VIP: @{name} ---")
        querystring = {"user_id": user_id, "count": "10"}
        
        try:
            response = requests.get(url_get_tweets, headers=headers, params=querystring)
            data = response.json()
            tweets = data.get('data') or data.get('results') or data.get('timeline')
            
            if not tweets or not isinstance(tweets, list):
                continue
                
            for item in tweets:
                clean_tweet = {
                    "id": item.get("id_str") or item.get("id"),
                    "text": item.get("full_text") or item.get("text", ""),
                    "created_at": item.get("created_at") or item.get("timestamp"),
                    "author": name, 
                    "target_coin": "WHALE_SIGNAL"
                }
                
                if clean_tweet["text"]:
                    producer.send(KAFKA_TOPIC, key=name, value=clean_tweet)
                    short_text = clean_tweet['text'][:60].replace('\n', ' ')
                    logger.info(f"-> [VIP: @{name}] Bơm: {short_text}...")
                    total_count += 1
        except Exception as e:
            logger.error(f"[LỖI] Xử lý @{name}: {e}")
        time.sleep(2)

    producer.flush()
    logger.info(f"\n[HOÀN TẤT] Đã bơm thành công {total_count} tín hiệu lên Kafka!")

if __name__ == "__main__":
    track_whales()