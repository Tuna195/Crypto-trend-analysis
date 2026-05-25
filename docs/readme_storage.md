# ----------- Test thu craw va luu tru du lieu ------------------------------------
craw --> Kafka --> HDFS

Buoc 1: Chay docker compose -f infrastructure/docker-compose.yml up -d

(Neu chi xem du lieu thi khong can buoc nay)
Buoc 2: Mo 1 tab moi va chay python src/ingestion/kafka_to_hdfs.py de luu du lieu tu kafka xuong hdfs
        Mo 1 tab moi de chay craw du lieu python src/ingestion/main_ingestion.py (HOAC python src/ingestion/twitter_client.py HOAC python src/ingestion/whale_client.py)

Buoc 3: Docker compose của chúng ta đã mở sẵn cổng Web cho HDFS. Bạn chỉ cần mở trình duyệt web lên và truy cập vào địa chỉ: http://localhost:9870 Sau đó, trên thanh menu phía trên, chọn Utilities -> Browse the file system và nhập đường dẫn /data/crypto để xem trực quan các thư mục và file. Bạn thậm chí có thể tải file về máy tính từ đây.


# ----------------------- Luu du lieu vao MongoDB ---------------------------------------

1. Batch Layer (Xử lý định kỳ bằng Spark)
Nằm ở file src/processing/batch_layer/batch_job.py.

Luồng: Spark đọc một lượng lớn dữ liệu thô từ HDFS, chạy mô hình để chấm điểm cảm xúc (Sentiment) cho các bài viết.
Cách lưu: Sau khi nhóm điểm số theo từng đồng coin, hệ thống sử dụng class MongoStorageClient (đã viết sẵn trong src/storage/mongo_client.py). Nó lặp qua từng kết quả và gọi hàm mongo.save_sentiment_metric().
Hàm này thực chất là thực hiện lệnh Insert (thêm mới) vào collection sentiment_metrics. Vì đây là công việc chạy định kỳ chốt số liệu (ví dụ: mỗi ngày 1 lần), nên việc insert mới hoàn toàn là hợp lý.

2. Speed Layer (Xử lý luồng thời gian thực bằng Spark Streaming)
Nằm ở file src/processing/speed_layer/stream_job.py.

Luồng: Spark Streaming hút dữ liệu liên tục từ Kafka, tính toán điểm xu hướng (trend_score, độ tương tác, số lượng người có ảnh hưởng nhắc tới...).
Cách lưu: Dữ liệu streaming được đẩy đi theo từng "mẻ nhỏ" (micro-batch) bằng cơ chế foreachBatch. Khi đó, nó sẽ gọi hàm write_batch_to_mongo().
Điểm đặc biệt (Kỹ thuật Upsert): Vì dữ liệu streaming được cập nhật từng giây từng phút, nếu dùng lệnh Insert bình thường thì có thể bị trùng lặp dữ liệu. Do đó, hàm này kết nối thẳng bằng thư viện pymongo và sử dụng cơ chế bulk_write với UpdateOne(..., upsert=True).
Cơ chế này hoạt động theo nguyên tắc: Tìm bản ghi có cùng symbol (đồng coin) và cùng khung thời gian (window_start & window_end). Nếu đã có thì cập nhật điểm số mới nhất, nếu chưa có thì tạo mới. Điều này vừa giúp thao tác cực nhanh trên hàng nghìn bản ghi cùng lúc, vừa đảm bảo tính không trùng lặp (Idempotency).
