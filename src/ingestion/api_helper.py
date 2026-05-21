import os
import time
import requests
from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '../../.env')
load_dotenv(dotenv_path=env_path, override=True)

# Lấy danh sách các key từ RAPIDAPI_KEYS (hoặc RAPIDAPI_KEY nếu bạn quên thêm chữ S)
raw_keys = os.getenv("RAPIDAPI_KEYS") or os.getenv("RAPIDAPI_KEY") or ""
RAPIDAPI_KEYS = raw_keys.split(",")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST")

def fetch_tweets_with_fallback(querystring, logger):
    """
    Hàm gọi API thông minh: Thử từng Key trong băng đạn. 
    Nếu Key chết/hết hạn (429/403) thì tự động thay Key mới.
    """
    url = f"https://{RAPIDAPI_HOST}/search.php"
    
    # Dọn dẹp mảng key (xóa khoảng trắng thừa)
    valid_keys = [k.strip() for k in RAPIDAPI_KEYS if k.strip()]
    
    if not valid_keys:
        logger.error("[LỖI NGHIÊM TRỌNG] Không tìm thấy RAPIDAPI_KEYS trong file .env!")
        return None

    for idx, key in enumerate(valid_keys):
        headers = {
            "x-rapidapi-key": key,
            "x-rapidapi-host": RAPIDAPI_HOST
        }
        
        try:
            logger.info(f"Đang thử kết nối API bằng Key #{idx + 1}...")
            response = requests.get(url, headers=headers, params=querystring)
            
            if response.status_code == 200:
                logger.info(f"-> [THÀNH CÔNG] Key #{idx + 1} vẫn còn xài tốt!")
                return response.json()
                
            elif response.status_code in [429, 403]:
                logger.warning(f"-> [HẾT ĐẠN] Key #{idx + 1} đã cạn kiệt request (Lỗi {response.status_code}). Nạp Key tiếp theo...")
                time.sleep(1)
                continue
                
            else:
                logger.error(f"-> [LỖI LẠ] Trạng thái {response.status_code}: {response.text}")
                time.sleep(1)
                continue
                
        except Exception as e:
            logger.error(f"-> [LỖI MẠNG] Gọi API bằng Key #{idx + 1} thất bại: {e}")
            time.sleep(1)
            continue
            
    logger.error("🚨 [THẤT BẠI TOÀN TẬP] Toàn bộ 5 Key của bạn đều đã hết hạn! Hãy nạp thêm Key mới vào .env!")
    return None
