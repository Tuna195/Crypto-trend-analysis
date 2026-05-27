FROM python:3.11-slim

WORKDIR /app

# Cài đặt Java (Cần thiết cho PySpark)
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Copy file requirements trước để tận dụng Docker cache
COPY requirements.txt .

# Cài đặt các thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ source code vào container
COPY . .

# Chạy lệnh mặc định (có thể bị ghi đè trong file k8s deployment)
CMD ["python", "src/ingestion/main_ingestion.py"]
