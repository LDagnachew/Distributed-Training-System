import argparse
import json
import os
import sys
import tempfile
import time
from typing import Optional, Tuple

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
    parser.add_argument("--checkpoint-interval-steps", type=int, default=50)
    # When set, the trainer loads the named checkpoint and resumes from that step.
    parser.add_argument("--resume-from-checkpoint", default=None,
                        help="S3 path to restore from, e.g. s3://bucket/prefix/run/checkpoint-25")
    return parser.parse_args()


def get_s3_client():
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
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    without_scheme = uri[len("s3://"):]
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) == 2 else ""
    return bucket, key


# Signal file — lets the worker-agent learn about checkpoint steps in real
# time without any IPC plumbing.  Path: $TMPDIR/{worker_id}.ckpt

def signal_file_path(worker_id: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"{worker_id}.ckpt")


def write_checkpoint_signal(worker_id: str, run_id: str, step: int) -> None:
    """Atomically update the signal file with run-scoped checkpoint metadata."""
    path = signal_file_path(worker_id)
    tmp = path + ".tmp"
    payload = {"run_id": run_id, "step": step}
    with open(tmp, "w") as f:
        f.write(json.dumps(payload))
    os.replace(tmp, path)  # atomic on POSIX
    print(f"[trainer] signal file updated: {path} → run_id={run_id} step {step}", flush=True)


def load_checkpoint(s3, checkpoint_uri: str) -> Optional[dict]:
    """Download checkpoint metadata.json from MinIO. Returns None on failure."""
    metadata_uri = checkpoint_uri.rstrip("/") + "/metadata.json"
    try:
        bucket, key = parse_s3_uri(metadata_uri)
        resp = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(resp["Body"].read().decode("utf-8"))
        print(f"[trainer] loaded checkpoint from s3://{bucket}/{key}", flush=True)
        return data
    except (BotoCoreError, ClientError, ValueError) as e:
        print(
            f"[trainer] WARNING: could not load checkpoint from {metadata_uri}: {e}",
            file=sys.stderr, flush=True,
        )
        return None


def write_checkpoint(s3, checkpoint_prefix: str, run_id: str, step: int, metadata: dict) -> bool:
    """
    Write checkpoint metadata to:
        {checkpoint_prefix}/{run_id}/checkpoint-{step}/metadata.json

    This layout must match coordinator::controller::build_checkpoint_path.
    """
    try:
        ckpt_bucket, ckpt_key_prefix = parse_s3_uri(checkpoint_prefix)
        key = f"{ckpt_key_prefix.rstrip('/')}/{run_id}/checkpoint-{step}/metadata.json"
        body = json.dumps(metadata).encode("utf-8")
        s3.put_object(Bucket=ckpt_bucket, Key=key, Body=body)
        print(f"[trainer] checkpoint written: s3://{ckpt_bucket}/{key}", flush=True)
        return True
    except (BotoCoreError, ClientError, ValueError) as e:
        print(f"[trainer] ERROR: failed to write checkpoint at step {step}: {e}",
              file=sys.stderr, flush=True)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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
        f"checkpoint_prefix={args.checkpoint_prefix} "
        f"interval_steps={args.checkpoint_interval_steps}",
        flush=True,
    )

    try:
        s3 = get_s3_client()
    except Exception as e:
        print(f"[trainer] ERROR: failed to create S3/MinIO client: {e}", file=sys.stderr, flush=True)
        return 1

    # ── Checkpoint restore ────────────────────────────────────────────────────
    start_step = 0
    if args.resume_from_checkpoint:
        print(f"[trainer] restoring from checkpoint: {args.resume_from_checkpoint}", flush=True)
        ckpt_data = load_checkpoint(s3, args.resume_from_checkpoint)
        if ckpt_data is not None:
            start_step = int(ckpt_data.get("step", 0))
            print(f"[trainer] resuming from step {start_step}", flush=True)
        else:
            print("[trainer] WARNING: checkpoint load failed, starting from step 0", flush=True)

    # ── Dataset connectivity check ────────────────────────────────────────────
    try:
        dataset_bucket, dataset_prefix = parse_s3_uri(args.dataset_uri)
        resp = s3.list_objects_v2(Bucket=dataset_bucket, Prefix=dataset_prefix, MaxKeys=5)
        contents = resp.get("Contents") or []
        if contents:
            print(f"[trainer] found {len(contents)} dataset objects (showing up to 5):", flush=True)
            for obj in contents[:5]:
                print(f"[trainer]   - s3://{dataset_bucket}/{obj['Key']}", flush=True)
        else:
            print(f"[trainer] WARNING: no dataset objects found under s3://{dataset_bucket}/{dataset_prefix}", flush=True)
    except (BotoCoreError, ClientError, ValueError) as e:
        print(f"[trainer] ERROR: dataset list failed: {e}", file=sys.stderr, flush=True)

    # ── Training loop ─────────────────────────────────────────────────────────
    total_steps = max(1, args.epochs * 50)

    if start_step >= total_steps:
        print(f"[trainer] already complete (start_step={start_step} >= total_steps={total_steps})", flush=True)
        return 0

    print(f"[trainer] running steps {start_step + 1}–{total_steps}", flush=True)

    checkpoint_metadata_base = {
        "run_id": args.run_id,
        "worker_id": args.worker_id,
        "rank": args.rank,
        "world_size": args.world_size,
        "model": args.model,
        "total_steps": total_steps,
    }

    for step in range(start_step + 1, total_steps + 1):
        loss = 1.0 / (1.0 + 0.05 * step)
        throughput = args.batch_size * (5 + step % 10)
        print(
            f"[trainer] step={step}/{total_steps} loss={loss:.4f} throughput={throughput}",
            flush=True,
        )
        time.sleep(0.1)

        if step % args.checkpoint_interval_steps == 0 or step == total_steps:
            metadata = {**checkpoint_metadata_base, "step": step}
            ok = write_checkpoint(s3, args.checkpoint_prefix, args.run_id, step, metadata)
            if ok:
                # Signal the worker-agent about the new checkpoint step so it
                # can report it to the coordinator even if the process is killed
                # before exiting cleanly.
                write_checkpoint_signal(args.worker_id, args.run_id, step)

    print("[trainer] completed training run successfully.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
