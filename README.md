# Crypto Trend Analysis

Big Data system to collect and analyze high-volume Twitter data for crypto market sentiment and early trend detection.

## Project Focus

The scope is narrowed to crypto trends, with these objectives:

- Crowd sentiment metrics: classify tweets as Bullish, Bearish, Neutral and compute realtime Fear & Greed proxy.
- Trend and gem detection: identify sudden mention spikes of coins/categories (AI, Layer2, Meme) and track influencer impact.
- Social-market correlation: combine social signal with market data (Binance/CoinGecko) for correlation and early alerts.

## Current Priority: Storage Layer

This repository now includes a storage-first implementation:

- HDFS client for raw tweet data in partitioned JSONL layout.
- MongoDB client for sentiment metrics, trend spikes, and alert records.
- Local Docker Compose setup for MongoDB + HDFS.
- Kubernetes manifests for storage deployment.

## Storage Structure

- `src/storage/hdfs_client.py`: WebHDFS operations and partitioned raw tweet storage.
- `src/storage/mongo_client.py`: MongoDB persistence and index creation.
- `infrastructure/k8s/storage`: Kubernetes YAML for namespace, MongoDB, and HDFS.
- `docs/storage_setup.md`: runbook for local and Kubernetes deployment.

## Quick Start (Storage)

Install dependencies:

```bash
pip install -r requirements.txt
```

Run local storage:

```bash
docker compose -f infrastructure/docker-compose.yml up -d
```

Deploy storage on Kubernetes:

```bash
kubectl apply -k infrastructure/k8s/storage
```

## Next Steps

- Connect ingestion pipeline to write raw tweets into HDFS.
- Connect processing pipeline to write sentiment metrics and alerts to MongoDB.
- Add backend API endpoints for dashboard queries.
