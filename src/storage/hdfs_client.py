"""HDFS helper utilities for storing crypto social stream data.

This client is intentionally lightweight and WebHDFS-based so it can run
inside ingestion/processing jobs without a local Hadoop binary.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from hdfs import InsecureClient
from hdfs.util import HdfsError


class HDFSClientError(RuntimeError):
	"""Raised when an HDFS operation fails."""


@dataclass
class HDFSConfig:
	"""Configuration for WebHDFS access."""

	webhdfs_url: str = os.getenv("WEBHDFS_URL", "http://namenode:9870")
	user: str = os.getenv("HDFS_USER", "hdfs")
	base_dir: str = os.getenv("HDFS_BASE_DIR", "/data/crypto")


class HDFSStorageClient:
	"""Convenience wrapper around the WebHDFS client."""

	def __init__(self, config: Optional[HDFSConfig] = None) -> None:
		self.config = config or HDFSConfig()
		self._client = InsecureClient(self.config.webhdfs_url, user=self.config.user)

	def healthcheck(self) -> bool:
		"""Return True when HDFS responds and root listing works."""
		try:
			self._client.list("/")
			return True
		except HdfsError:
			return False

	def ensure_dir(self, hdfs_dir: str) -> None:
		"""Create target directory (recursive) if missing."""
		try:
			self._client.makedirs(hdfs_dir)
		except HdfsError as exc:
			raise HDFSClientError(f"Cannot create HDFS directory '{hdfs_dir}': {exc}") from exc

	def write_json_lines(
		self,
		records: Iterable[dict],
		hdfs_path: str,
		overwrite: bool = True,
	) -> None:
		"""Write an iterable of dictionaries as JSON Lines file into HDFS."""
		try:
			payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
			with self._client.write(hdfs_path, overwrite=overwrite, encoding="utf-8") as writer:
				writer.write(payload)
				if payload:
					writer.write("\n")
		except HdfsError as exc:
			raise HDFSClientError(f"Cannot write file '{hdfs_path}' to HDFS: {exc}") from exc

	def append_json_lines(self, records: Iterable[dict], hdfs_path: str) -> None:
		"""Append JSON Lines records when the target file already exists."""
		try:
			payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
			if not payload:
				return
			with self._client.write(hdfs_path, append=True, encoding="utf-8") as writer:
				writer.write(payload)
				writer.write("\n")
		except HdfsError as exc:
			raise HDFSClientError(f"Cannot append file '{hdfs_path}' in HDFS: {exc}") from exc

	def read_json_lines(self, hdfs_path: str) -> list[dict]:
		"""Read a JSON Lines file from HDFS and return list of dictionaries."""
		try:
			with self._client.read(hdfs_path, encoding="utf-8") as reader:
				return [json.loads(line) for line in reader if line.strip()]
		except HdfsError as exc:
			raise HDFSClientError(f"Cannot read file '{hdfs_path}' from HDFS: {exc}") from exc

	def store_raw_tweets(
		self,
		coin_symbol: str,
		tweets: Iterable[dict],
		event_time: Optional[datetime] = None,
	) -> str:
		"""Store tweet batch into partitioned HDFS path and return file path.

		Partitioning format:
		/data/crypto/raw_tweets/coin=BTC/date=2026-04-18/hour=15/20260418T150010Z.jsonl
		"""
		ts = event_time or datetime.utcnow()
		coin = coin_symbol.upper().replace("$", "")
		partition_dir = (
			f"{self.config.base_dir}/raw_tweets"
			f"/coin={coin}/date={ts:%Y-%m-%d}/hour={ts:%H}"
		)
		self.ensure_dir(partition_dir)
		file_path = f"{partition_dir}/{ts:%Y%m%dT%H%M%SZ}.jsonl"
		self.write_json_lines(tweets, file_path, overwrite=True)
		return file_path

	def upload_local_file(self, local_path: str, hdfs_path: str, overwrite: bool = True) -> None:
		"""Upload local file (e.g. model artifact, CSV snapshot) to HDFS."""
		local = Path(local_path)
		if not local.exists():
			raise HDFSClientError(f"Local file does not exist: {local_path}")
		try:
			self._client.upload(hdfs_path, str(local), overwrite=overwrite)
		except HdfsError as exc:
			raise HDFSClientError(f"Cannot upload '{local_path}' to '{hdfs_path}': {exc}") from exc

