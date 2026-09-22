from __future__ import annotations

import os
import sys
from pathlib import Path

import boto3
from botocore.config import Config


# >>> download every object under an s3 prefix into a local directory and print a listing <<< #
def main(uri: str, out_dir: str) -> None:
    bucket, _, prefix = uri.removeprefix("s3://").partition("/")
    prefix = prefix.rstrip("/") + "/" if prefix.strip("/") else ""
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["DTRACK_S3_ENDPOINT_URL"],
        config=Config(s3={"addressing_style": "path"}),
    )
    root = Path(out_dir)
    total = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for row in sorted(page.get("Contents") or [], key=lambda item: item["Key"]):
            relative = row["Key"].removeprefix(prefix).lstrip("/")
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, row["Key"], str(target))
            total += 1
            print(f"{row['Size']:>8}  s3://{bucket}/{row['Key']}  ->  {target}")
    print(f"{total} object(s) mirrored under {root}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
