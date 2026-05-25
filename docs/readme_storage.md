Test thu craw va luu tru du lieu
craw --> Kafka --> HDFS

Buoc 1: Chay docker compose -f infrastructure/docker-compose.yml up -d

(Neu chi xem du lieu thi khong can buoc nay)
Buoc 2: Mo 1 tab moi va chay python src/ingestion/kafka_to_hdfs.py de luu du lieu tu kafka xuong hdfs
        Mo 1 tab moi de chay craw du lieu python src/ingestion/main_ingestion.py (HOAC python src/ingestion/twitter_client.py HOAC python src/ingestion/whale_client.py)

Buoc 3: Docker compose của chúng ta đã mở sẵn cổng Web cho HDFS. Bạn chỉ cần mở trình duyệt web lên và truy cập vào địa chỉ: http://localhost:9870 Sau đó, trên thanh menu phía trên, chọn Utilities -> Browse the file system và nhập đường dẫn /data/crypto để xem trực quan các thư mục và file. Bạn thậm chí có thể tải file về máy tính từ đây.