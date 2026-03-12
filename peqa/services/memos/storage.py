from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from flask import current_app

from models import MemoStoredBlob, db


class DocumentStorage:
    def put(self, file_bytes: bytes, key: str) -> str:
        raise NotImplementedError

    def get(self, key: str) -> bytes:
        raise NotImplementedError

    def delete(self, key: str) -> None:
        raise NotImplementedError

    def signed_url(self, key: str) -> str | None:
        return None


class LocalDocumentStorage(DocumentStorage):
    def __init__(self, root: str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _full_path(self, key: str) -> Path:
        normalized = key.strip("/").replace("..", "_")
        return self.root / normalized

    def put(self, file_bytes: bytes, key: str) -> str:
        path = self._full_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(file_bytes)
        return key

    def get(self, key: str) -> bytes:
        return self._full_path(key).read_bytes()

    def delete(self, key: str) -> None:
        path = self._full_path(key)
        if path.exists():
            path.unlink()

    def signed_url(self, key: str) -> str | None:
        return str(self._full_path(key))


class S3DocumentStorage(DocumentStorage):
    def __init__(self, config: dict[str, Any]):
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("boto3 is required for S3 memo storage") from exc

        self.bucket = config["MEMO_S3_BUCKET"]
        self.client = boto3.client(
            "s3",
            region_name=config.get("MEMO_S3_REGION"),
            endpoint_url=config.get("MEMO_S3_ENDPOINT_URL"),
            aws_access_key_id=config.get("MEMO_S3_ACCESS_KEY_ID"),
            aws_secret_access_key=config.get("MEMO_S3_SECRET_ACCESS_KEY"),
        )

    def put(self, file_bytes: bytes, key: str) -> str:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=file_bytes)
        return key

    def get(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def signed_url(self, key: str) -> str | None:
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=3600,
        )


class DatabaseDocumentStorage(DocumentStorage):
    def put(self, file_bytes: bytes, key: str) -> str:
        row = MemoStoredBlob.query.filter_by(storage_key=key).first()
        if row is None:
            row = MemoStoredBlob(storage_key=key, content=file_bytes, size_bytes=len(file_bytes))
        else:
            row.content = file_bytes
            row.size_bytes = len(file_bytes)
        db.session.add(row)
        db.session.commit()
        return key

    def get(self, key: str) -> bytes:
        row = MemoStoredBlob.query.filter_by(storage_key=key).first()
        if row is None:
            raise FileNotFoundError(f"Memo blob not found for key: {key}")
        return bytes(row.content or b"")

    def delete(self, key: str) -> None:
        row = MemoStoredBlob.query.filter_by(storage_key=key).first()
        if row is not None:
            db.session.delete(row)
            db.session.commit()


def get_document_storage(config: dict[str, Any] | None = None) -> DocumentStorage:
    active_config = config or current_app.config
    backend = (active_config.get("MEMO_STORAGE_BACKEND") or "local").strip().lower()
    if backend == "s3":
        return S3DocumentStorage(active_config)
    if backend == "db":
        return DatabaseDocumentStorage()
    return LocalDocumentStorage(active_config["MEMO_STORAGE_LOCAL_ROOT"])


def build_storage_key(*parts: str) -> str:
    sanitized = []
    for part in parts:
        token = (part or "").strip().strip("/")
        if token:
            sanitized.append(token)
    return "/".join(sanitized)
