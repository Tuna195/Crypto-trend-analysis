# =============================================================================
# Dockerfile — Image cho layer Ingestion
#
# Build image:
#   docker build -t crypto-ingestion:latest .
#
# Load vào Kind cluster:
#   kind load docker-image crypto-ingestion:latest --name <tên-cluster>
#
# Kiểm tra image đã build xong:
#   docker images | grep crypto-ingestion
# =============================================================================

# ── BƯỚC 1: Chọn "hộp nền" (Base Image) ──────────────────────────────────────
# Giống như chọn hệ điều hành cài sẵn Python 3.11.
# "-slim" là phiên bản nhẹ (không có compiler, không có tool thừa).
# Kết quả: image sẽ nhẹ hơn ~300MB so với bản đầy đủ.
FROM python:3.11-slim

# ── BƯỚC 2: Cài thư viện hệ thống (nếu cần) ──────────────────────────────────
# procps: cung cấp lệnh "pgrep" — được dùng trong livenessProbe của kafka-to-hdfs
# Sau khi cài xong, xóa cache apt để giảm kích thước image
RUN apt-get update && \
    apt-get install -y --no-install-recommends procps && \
    rm -rf /var/lib/apt/lists/*

# ── BƯỚC 3: Đặt thư mục làm việc bên trong container ─────────────────────────
# Tất cả lệnh sau sẽ chạy trong /app thay vì root /
# Giống như "cd /app" trước khi chạy mọi thứ
WORKDIR /app

# ── BƯỚC 4: Copy requirements TRƯỚC (tách riêng để tận dụng Docker cache) ────
# Lý do copy requirements trước khi copy code:
#   - Docker build theo từng "layer" (lớp), mỗi dòng là 1 layer
#   - Nếu requirements-ingestion.txt không đổi, Docker dùng lại cache pip install
#   - Chỉ khi thay đổi code (src/) thì các bước sau mới chạy lại
#   - Điều này giúp rebuild nhanh hơn rất nhiều khi dev
COPY requirements-ingestion.txt .

# ── BƯỚC 5: Cài thư viện Python ───────────────────────────────────────────────
# --no-cache-dir: không lưu cache pip vào image → giảm kích thước
# --upgrade pip: đảm bảo pip mới nhất
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements-ingestion.txt

# ── BƯỚC 6: Copy source code vào image ───────────────────────────────────────
# Chỉ copy thư mục src/ (chứa code ingestion và storage client)
# KHÔNG copy: .env, certs/, data/, docs/, __pycache__/ (xem .dockerignore)
COPY src/ ./src/

# ── BƯỚC 7: Tạo user non-root để chạy ────────────────────────────────────────
# Best practice bảo mật: không chạy process bằng root bên trong container
# Tương tự việc không dùng Administrator để chạy app thông thường
RUN useradd --no-create-home --shell /bin/false appuser && \
    chown -R appuser:appuser /app
USER appuser

# ── GHI CHÚ: Không có CMD mặc định ───────────────────────────────────────────
# Image này được dùng chung cho nhiều script khác nhau.
# Lệnh chạy được chỉ định trong từng manifest YAML (command: [...])
#
#   historical-scraper Job  → command: ["python", "src/ingestion/historical_scraper.py"]
#   twitter_client CronJob  → command: ["python", "src/ingestion/twitter_client.py"]
#   whale CronJob           → command: ["python", "src/ingestion/whale_client.py"]
#   kafka-to-hdfs Deploy    → command: ["python", "src/ingestion/kafka_to_hdfs.py"]
