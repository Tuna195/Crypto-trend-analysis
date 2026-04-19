import logging
import os


current_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(current_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, 'ingestion.log')

# Format log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-12s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        # Ghi âm thầm vào file text
        logging.FileHandler(log_file, encoding='utf-8'),
        # Vẫn in ra màn hình Terminal để bạn dễ nhìn
        logging.StreamHandler() 
    ]
)

def get_logger(module_name):
    """Hàm cấp phát sổ nhật ký cho từng file riêng biệt"""
    return logging.getLogger(module_name)