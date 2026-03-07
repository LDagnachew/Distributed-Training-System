import argparse
import json
import os
import sys
import time
from typing import Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MVP Python training stub for Distributed Trainer (MinIO-backed)."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--rank", type=int, required=True)
    parser.add_argument("--world-size", type=int, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--learning-rate", type=float, required=True)
    parser.add_argument("--batch-size", type=int, required=True)
    parser.add_argument("--epochs", type=int, required=True)
    parser.add_argument("--dataset-uri", required=True)
    parser.add_argument("--checkpoint-prefix", required=True)
    return parser.parse_args()


def get_s3_client():
    """
    Create a boto3 S3 client configured for MinIO using environment variables.

    Expected env vars (with safe local defaults):
      - MINIO_ENDPOINT (default: http://localhost:9000)
      - MINIO_ACCESS_KEY (default: admin)
      - MINIO_SECRET_KEY (default: adminadmin)
      - MINIO_REGION (default: us-east-1)
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "adminadmin")
    region = os.getenv("MINIO_REGION", "us-east-1")

    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """
    Very small S3 URI parser compatible with the Rust side.
    Accepts strings like: s3://bucket/path/to/prefix/
    Returns (bucket, key_prefix).
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {uri}")

    without_scheme = uri[len("s3://") :]
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) == 2 else ""
    return bucket, key


def main() -> int:
    args = parse_args()

    print(
        f"[trainer] start "
        f"run_id={args.run_id} worker_id={args.worker_id} "
        f"rank={args.rank}/{args.world_size} "
        f"model={args.model} lr={args.learning_rate} "
        f"batch_size={args.batch_size} epochs={args.epochs}",
        flush=True,
    )
    print(
        f"[trainer] dataset_uri={args.dataset_uri} "
        f"checkpoint_prefix={args.checkpoint_prefix}",
        flush=True,
    )

    # Connect to MinIO via boto3.
    try:
        s3 = get_s3_client()
    except Exception as e:  # pragma: no cover - defensive
        print(f"[trainer] ERROR: failed to create S3/MinIO client: {e}", file=sys.stderr, flush=True)
        return 1

    # Log a small sample of dataset objects to prove connectivity.
    try:
        dataset_bucket, dataset_prefix = parse_s3_uri(args.dataset_uri)
        resp = s3.list_objects_v2(
            Bucket=dataset_bucket,
            Prefix=dataset_prefix,
            MaxKeys=5,
        )
        contents = resp.get("Contents") or []
        if contents:
            print(
                f"[trainer] found {len(contents)} dataset objects under "
                f"s3://{dataset_bucket}/{dataset_prefix} (showing up to 5):",
                flush=True,
            )
            for obj in contents[:5]:
                print(f"[trainer]   - s3://{dataset_bucket}/{obj['Key']}", flush=True)
        else:
            print(
                f"[trainer] WARNING: no dataset objects found under "
                f"s3://{dataset_bucket}/{dataset_prefix}",
                flush=True,
            )
    except (BotoCoreError, ClientError, ValueError) as e:
        print(f"[trainer] ERROR: failed to list dataset objects from MinIO: {e}", file=sys.stderr, flush=True)
        # Continue anyway; training loop below is still useful for testing.

    # Minimal simulated training loop: we just emit some fake loss values
    # and sleep to approximate work. This keeps all correctness work
    # on the Rust side while giving a realistic control-plane boundary.
    total_steps = max(1, args.epochs * 50)
    for step in range(1, total_steps + 1):
        # Simple decreasing loss curve
        loss = 1.0 / (1.0 + 0.05 * step)
        throughput = args.batch_size * (5 + step % 10)
        print(
            f"[trainer] step={step}/{total_steps} "
            f"loss={loss:.4f} "
            f"throughput_samples_per_sec={throughput}",
            flush=True,
        )
        time.sleep(0.1)

    # Write a tiny checkpoint marker object to MinIO under the checkpoint prefix.
    try:
        ckpt_bucket, ckpt_prefix = parse_s3_uri(args.checkpoint_prefix)
        ckpt_prefix = ckpt_prefix.rstrip("/")
        marker_key = f"{ckpt_prefix}/worker_{args.worker_id}_step_{total_steps}.json"
        body = json.dumps(
            {
                "run_id": args.run_id,
                "worker_id": args.worker_id,
                "rank": args.rank,
                "world_size": args.world_size,
                "model": args.model,
                "epochs": args.epochs,
                "total_steps": total_steps,
            }
        ).encode("utf-8")

        s3.put_object(Bucket=ckpt_bucket, Key=marker_key, Body=body)
        print(
            f"[trainer] wrote checkpoint marker to "
            f"s3://{ckpt_bucket}/{marker_key}",
            flush=True,
        )
    except (BotoCoreError, ClientError, ValueError) as e:
        print(f"[trainer] ERROR: failed to write checkpoint marker to MinIO: {e}", file=sys.stderr, flush=True)
        return 1

    print("[trainer] completed training run successfully.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

