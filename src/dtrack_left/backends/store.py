from __future__ import annotations

import json
import os
import posixpath
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


def validate_run_id(value: object) -> str:
    run_id = str(value or "")
    if not run_id or run_id in {".", ".."} or "/" in run_id or "\\" in run_id or "\0" in run_id:
        raise ValueError("run_id must be one non-empty folder name below the runs root")
    return run_id


@dataclass(frozen=True)
class S3Location:
    bucket: str
    prefix: str = ""

    @classmethod
    def parse(cls, uri: str) -> S3Location:
        parsed = urlparse(uri)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError(f"expected s3://bucket/prefix, got {uri!r}")
        return cls(parsed.netloc, parsed.path.strip("/"))


class S3Store:
    def __init__(self, root_uri: str, *, client=None):
        self.root_uri = root_uri
        self.location = S3Location.parse(root_uri)
        if client is None:
            import boto3
            from botocore.config import Config

            endpoint = os.getenv("DTRACK_S3_ENDPOINT_URL") or None
            kwargs: dict[str, Any] = {}
            if endpoint:
                kwargs.update(
                    endpoint_url=endpoint,
                    config=Config(s3={"addressing_style": "path"}),
                )
            client = boto3.client("s3", **kwargs)
        self.client = client

    @property
    def bucket(self) -> str:
        return self.location.bucket

    def _key(self, relative: str) -> str:
        relative = relative.lstrip("/")
        return posixpath.join(self.location.prefix, relative) if self.location.prefix else relative

    def _relative(self, key: str) -> str:
        prefix = f"{self.location.prefix}/" if self.location.prefix else ""
        return key.removeprefix(prefix)

    @staticmethod
    def json_bytes(value: Any) -> bytes:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()

    def put_bytes(self, relative: str, body: bytes, *, content_type: str) -> str:
        kwargs = {
            "Bucket": self.bucket,
            "Key": self._key(relative),
            "Body": body,
            "ContentType": content_type,
        }
        self.client.put_object(**kwargs)
        return kwargs["Key"]

    def put_json(self, relative: str, value: Any) -> str:
        return self.put_bytes(relative, self.json_bytes(value), content_type="application/json")

    def get_bytes(self, relative: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self._key(relative))
        return response["Body"].read()

    def get_json(self, relative: str) -> Any:
        return json.loads(self.get_bytes(relative).decode())

    def exists(self, relative: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(relative))
            return True
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"} or "not found" in str(exc).lower():
                return False
            raise

    def list_keys(self, prefix: str, *, start_after: str | None = None) -> list[str]:
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": self._key(prefix)}
        if start_after:
            kwargs["StartAfter"] = self._key(start_after)
        result = []
        while True:
            response = self.client.list_objects_v2(**kwargs)
            result.extend(
                self._relative(row["Key"])
                for row in response.get("Contents") or []
                if not row["Key"].endswith("/")
            )
            if not response.get("IsTruncated"):
                return result
            kwargs.pop("StartAfter", None)
            kwargs["ContinuationToken"] = response["NextContinuationToken"]

    def list_prefixes(self, prefix: str = "") -> list[str]:
        base = self._key(prefix)
        base = base + "/" if base and not base.endswith("/") else base
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": base, "Delimiter": "/"}
        result = []
        while True:
            response = self.client.list_objects_v2(**kwargs)
            result.extend(
                row["Prefix"].removeprefix(base).strip("/")
                for row in response.get("CommonPrefixes") or []
            )
            if not response.get("IsTruncated"):
                return sorted(result)
            kwargs["ContinuationToken"] = response["NextContinuationToken"]

    def child(self, name: str) -> S3Store:
        return S3Store(self.root_uri.rstrip("/") + "/" + name, client=self.client)

    def delete_keys(self, keys: list[str]) -> int:
        deleted = 0
        for offset in range(0, len(keys), 1000):
            batch = keys[offset : offset + 1000]
            if not batch:
                continue
            response = self.client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": self._key(key)} for key in batch], "Quiet": True},
            )
            errors = response.get("Errors") or []
            if errors:
                raise RuntimeError(f"failed to delete audit objects: {errors}")
            deleted += len(batch)
        return deleted

    def list_json(self, prefix: str, *, start_after: str | None = None) -> list[tuple[str, Any]]:
        return [
            (key, self.get_json(key)) for key in self.list_keys(prefix, start_after=start_after)
        ]

    def commit_unit(
        self,
        sequence: int,
        unit_id: str,
        payload: dict[str, Any],
        pointer: dict[str, Any],
    ) -> dict[str, Any]:
        payload_key = f"units/{unit_id}.json"
        self.put_json(payload_key, payload)
        document = {
            **pointer,
            "unit_id": unit_id,
            "payload_key": payload_key,
            "sequence": int(sequence),
        }
        self.put_json(f"_INDEX/{sequence:08d}-{unit_id}.json", document)
        return document

    def heartbeat(self, now: float | None = None) -> str:
        return self.put_json("_HEARTBEAT", {"epoch_s": time.time() if now is None else now})

    def heartbeat_age(self, now: float | None = None) -> float | None:
        if not self.exists("_HEARTBEAT"):
            return None
        current = time.time() if now is None else now
        return max(0.0, current - float(self.get_json("_HEARTBEAT")["epoch_s"]))

    def close_manifests(self, pairs: list[str], expected_units: int) -> str:
        return self.put_json(
            "manifest/_CLOSED", {"pairs": sorted(pairs), "expected_units": expected_units}
        )

    def close_row_check(self, pairs: list[str]) -> str:
        return self.put_json("row_check/_CLOSED", {"pairs": sorted(pairs)})

    def put_result(self, unit_id: str, result: dict[str, Any]) -> str:
        return self.put_json(f"results/{unit_id}.json", result)

    def put_dead(self, unit_id: str, failure: dict[str, Any]) -> str:
        return self.put_json(f"dead/{unit_id}.json", failure)

    def ready_unclaimed(self) -> int:
        ready = len(self.list_keys("_INDEX/"))
        done = len(self.list_keys("results/")) + len(self.list_keys("dead/"))
        return max(0, ready - done)
