# Storage Setup (MongoDB + HDFS)

This project stores data in two tiers:

- HDFS: raw tweet stream (JSONL partitions by coin/date/hour)
- MongoDB: processed indicators, trend spikes, and alert events

## 1) Python Storage Layer

Implemented modules:

- `src/storage/hdfs_client.py`
- `src/storage/mongo_client.py`

Environment variables:

- `WEBHDFS_URL` (default: `http://namenode:9870`)
- `HDFS_USER` (default: `hdfs`)
- `HDFS_BASE_DIR` (default: `/data/crypto`)
- `MONGO_URI` (default: `mongodb://mongodb:27017`)
- `MONGO_DB` (default: `crypto_trends`)

## 2) Run Storage Locally with Docker Compose

```bash
docker compose -f infrastructure/docker-compose.yml up -d
```

Health checks:

- MongoDB service: `localhost:27017`
- NameNode UI: `http://localhost:9870`
- DataNode UI: `http://localhost:9864`

## 3) Deploy Storage on Kubernetes

Apply all manifests:

```bash
kubectl apply -k infrastructure/k8s/storage
```

Check status:

```bash
kubectl get pods -n crypto-trend
kubectl get pvc -n crypto-trend
kubectl get svc -n crypto-trend
```

## 4) Initialize MongoDB Indexes

Call in your startup code (ingestion/processing/backend):

```python
from src.storage.mongo_client import MongoStorageClient

mongo = MongoStorageClient()
mongo.ensure_indexes()
```

## 5) Example HDFS Write for Raw Tweets

```python
from src.storage.hdfs_client import HDFSStorageClient

hdfs = HDFSStorageClient()
hdfs.store_raw_tweets(
    coin_symbol="BTC",
    tweets=[
        {"tweet_id": "1", "text": "$BTC breakout", "sentiment": "Bullish"},
        {"tweet_id": "2", "text": "$BTC correction", "sentiment": "Bearish"},
    ],
)
```
