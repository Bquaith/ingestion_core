from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import gzip
import io
import json
from typing import Any, Iterator, TextIO


@dataclass(frozen=True)
class ObjectStoreConfig:
    bucket: str
    prefix: str = ""
    endpoint_url: str | None = None
    region_name: str | None = None
    verify_ssl: bool = True
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None

    def normalize_key(self, key: str) -> str:
        trimmed_key = key.strip().strip("/")
        trimmed_prefix = self.prefix.strip().strip("/")
        if not trimmed_key:
            raise ValueError("object store key must not be empty")
        if trimmed_prefix and (trimmed_key == trimmed_prefix or trimmed_key.startswith(f"{trimmed_prefix}/")):
            return trimmed_key
        if not trimmed_prefix:
            return trimmed_key
        return f"{trimmed_prefix}/{trimmed_key}"


class ObjectStoreClient:
    def __init__(self, config: ObjectStoreConfig) -> None:
        import boto3
        from botocore.config import Config as BotoConfig

        self.config = config
        self._client = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            region_name=config.region_name,
            verify=config.verify_ssl,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            aws_session_token=config.session_token,
            config=BotoConfig(signature_version="s3v4"),
        )

    def upload_file(
        self,
        file_path: str,
        key: str,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> str:
        normalized_key = self.config.normalize_key(key)
        extra_args: dict[str, str] = {}
        if content_type:
            extra_args["ContentType"] = content_type
        if content_encoding:
            extra_args["ContentEncoding"] = content_encoding
        upload_kwargs: dict[str, Any] = {
            "Filename": file_path,
            "Bucket": self.config.bucket,
            "Key": normalized_key,
        }
        if extra_args:
            upload_kwargs["ExtraArgs"] = extra_args
        self._client.upload_file(**upload_kwargs)
        return normalized_key

    def put_json(self, key: str, payload: dict[str, Any]) -> str:
        normalized_key = self.config.normalize_key(key)
        body = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
        self._client.put_object(
            Bucket=self.config.bucket,
            Key=normalized_key,
            Body=body,
            ContentType="application/json",
        )
        return normalized_key

    def get_json(self, key: str) -> dict[str, Any]:
        normalized_key = self.config.normalize_key(key)
        response = self._client.get_object(Bucket=self.config.bucket, Key=normalized_key)
        body = response["Body"]
        try:
            payload = json.loads(body.read().decode("utf-8"))
        finally:
            body.close()

        if not isinstance(payload, dict):
            raise ValueError(f"Object store JSON payload must be an object for key: {normalized_key}")
        return payload

    def object_exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        normalized_key = self.config.normalize_key(key)
        try:
            self._client.head_object(Bucket=self.config.bucket, Key=normalized_key)
        except ClientError as exc:
            error_code = str(exc.response.get("Error", {}).get("Code", ""))
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def copy_object(self, source_key: str, destination_key: str) -> str:
        normalized_source_key = self.config.normalize_key(source_key)
        normalized_destination_key = self.config.normalize_key(destination_key)
        self._client.copy_object(
            Bucket=self.config.bucket,
            Key=normalized_destination_key,
            CopySource={
                "Bucket": self.config.bucket,
                "Key": normalized_source_key,
            },
        )
        return normalized_destination_key

    @contextmanager
    def open_gzip_text_reader(self, key: str) -> Iterator[TextIO]:
        normalized_key = self.config.normalize_key(key)
        response = self._client.get_object(Bucket=self.config.bucket, Key=normalized_key)
        body = response["Body"]
        try:
            with gzip.GzipFile(fileobj=body, mode="rb") as gz_file:
                text_stream = io.TextIOWrapper(gz_file, encoding="utf-8")
                try:
                    yield text_stream
                finally:
                    text_stream.detach()
        finally:
            body.close()
