"""
landsat_download_files.py

Download Earth Engine image exports (e.g., Landsat NDVI/NDFI tiles) from
Google Cloud Storage (GCS) to a local folder.

Auth options:
  - Use gcloud ADC:
      gcloud auth application-default login --no-launch-browser
      gcloud auth application-default set-quota-project <PROJECT_ID>
  - Or set env var for service account:
      export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Optional, Union

try:
    import ee  # only needed if you pass live ee tasks
except Exception:
    ee = None  # noqa

from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.oauth2 import service_account


# ---------- helpers ----------

def parse_gs_url(gs_url: str) -> tuple[str, str]:
    if not gs_url.startswith("gs://"):
        raise ValueError(f"Expected gs:// URL, got: {gs_url!r}")
    s = gs_url[5:]
    parts = s.split("/", 1)
    bucket = parts[0]
    obj = parts[1] if len(parts) == 2 else ""
    if not obj:
        raise ValueError(f"No object path in GS URL: {gs_url!r}")
    return bucket, obj


def task_to_gsurl(task) -> tuple[str, str] | None:
    """Turn an ee Task into (description, gs://bucket/prefix.tif). Skip non-GCS tasks."""
    cfg = getattr(task, "config", None) or getattr(task, "_config", None)
    if not cfg:
        return None
    desc = cfg.get("description", "unnamed_task")
    export_opts = cfg.get("fileExportOptions", {})
    dst = export_opts.get("gcsDestination") or export_opts.get("cloudStorageDestination")
    if not dst:
        return None
    bucket = dst["bucket"]
    prefix = dst["filenamePrefix"]
    return desc, f"gs://{bucket}/{prefix}.tif"


def jsonentry_to_gsurl(item: tuple[str, dict]) -> tuple[str, str] | None:
    """Turn an export_dst.json entry into (description, gs://bucket/prefix.tif). Skip non-GCS."""
    desc, export_opts = item
    dst = export_opts.get("gcsDestination") or export_opts.get("cloudStorageDestination")
    if not dst:
        return None
    bucket = dst["bucket"]
    prefix = dst["filenamePrefix"]
    return desc, f"gs://{bucket}/{prefix}.tif"


def collect_urls(tasks: Union[str, Path, dict, Iterable]) -> list[tuple[str, str]]:
    """Normalize tasks (path|dict|iterable of tasks) → list[(desc, gs_url)]."""
    urls: list[tuple[str, str]] = []
    if isinstance(tasks, (str, Path)):
        with open(tasks, "r") as f:
            export_dst = json.load(f)
        for item in export_dst.items():
            pair = jsonentry_to_gsurl(item)
            if pair:
                urls.append(pair)
        return urls
    if isinstance(tasks, dict):
        for item in tasks.items():
            pair = jsonentry_to_gsurl(item)
            if pair:
                urls.append(pair)
        return urls
    # iterable of ee tasks
    for t in tasks:
        pair = task_to_gsurl(t)
        if pair:
            urls.append(pair)
    return urls


def make_storage_client(credentials_json: Optional[str], project: Optional[str]) -> storage.Client:
    """Create a Storage client using either a service-account file or ADC + project."""
    if credentials_json:
        creds = service_account.Credentials.from_service_account_file(credentials_json)
        return storage.Client(credentials=creds, project=project or creds.project_id)
    return storage.Client(project=project or os.environ.get("GOOGLE_CLOUD_PROJECT"))


def download_blob_or_shards(
    client: storage.Client,
    bucket_name: str,
    object_name: str,
    out_dir: Path,
    *,
    overwrite: bool,
    timeout: float = 600.0,
    chunk_mb: int = 8,
) -> list[Path]:
    """
    Try downloading a single file first. If NotFound and the name ends with .tif,
    download all shards matching <prefix>-*.tif.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / Path(object_name).name

    blob = client.bucket(bucket_name).blob(object_name)
    blob.chunk_size = chunk_mb * 1024 * 1024

    if out_path.exists() and not overwrite:
        return [out_path]

    try:
        blob.download_to_filename(str(out_path), timeout=timeout)
        return [out_path]
    except NotFound:
        # Fallback to sharded objects (EE may output prefix-0000000000.tif, etc.)
        if not object_name.lower().endswith(".tif"):
            raise

        prefix_no_ext = object_name[:-4]  # strip ".tif"
        shard_prefix = prefix_no_ext + "-"
        blobs = list(client.bucket(bucket_name).list_blobs(prefix=shard_prefix))
        if not blobs:
            # Nothing to fetch
            raise

        written: list[Path] = []
        for b in blobs:
            shard_out = out_dir / Path(b.name).name
            if shard_out.exists() and not overwrite:
                written.append(shard_out)
                continue
            b.chunk_size = chunk_mb * 1024 * 1024
            b.download_to_filename(str(shard_out), timeout=timeout)
            written.append(shard_out)
        return written


# ---------- public API ----------

def download_files(
    data_dir: Union[str, Path],
    tasks: Union[str, Path, dict, Iterable],
    *,
    overwrite: bool = True,
    workers: int = 8,
    mirror_bucket_path: bool = False,
    name_filter_regex: str | None = None,
    credentials_json: str | None = None,
    project: str | None = None,
    timeout: float = 600.0,
    retries: int = 2,
    retry_sleep: float = 3.0,
    chunk_mb: int = 8,
) -> list[Path]:
    """
    Download all files referenced by `tasks` to `data_dir`.

    Args:
        data_dir: Local output directory.
        tasks: list of ee Tasks, or path to export_dst.json, or dict loaded from JSON.
        overwrite: Overwrite existing local files.
        workers: Parallel download threads.
        mirror_bucket_path: If True, recreate bucket subpaths under data_dir.
        name_filter_regex: Regex applied to *description* to include subset (e.g., r"landsat_ndvi_.*_2024").
        credentials_json: Path to service account key JSON (optional; otherwise ADC is used).
        project: GCP project ID for ADC (avoids "project not determined").
        timeout: Per-file download timeout (seconds).
        retries: Retry count on NotFound (races right after EE completes).
        retry_sleep: Seconds to sleep between retries.
        chunk_mb: Download chunk size in MB (tune for network).
    """
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    items = collect_urls(tasks)
    if name_filter_regex:
        pat = re.compile(name_filter_regex)
        items = [(d, u) for (d, u) in items if pat.search(d)]
        if not items:
            print(f"[download_files] No items matched regex: {name_filter_regex!r}")
            return []

    client = make_storage_client(credentials_json, project)
    results: list[Path] = []
    failures: list[tuple[str, str]] = []

    def one(desc: str, gs_url: str) -> tuple[str, list[Path] | None, str | None]:
        bucket_name, object_name = parse_gs_url(gs_url)
        out_dir = (data_dir / object_name).parent if mirror_bucket_path else data_dir

        last_err = None
        for _ in range(max(1, retries + 1)):
            try:
                written = download_blob_or_shards(
                    client,
                    bucket_name,
                    object_name,
                    out_dir,
                    overwrite=overwrite,
                    timeout=timeout,
                    chunk_mb=chunk_mb,
                )
                return desc, written, None
            except NotFound as e:
                last_err = f"NotFound: {e}"
                time.sleep(retry_sleep)
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                break
        return desc, None, last_err

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(one, desc, gs) for desc, gs in items]
        for fut in as_completed(futs):
            desc, written, err = fut.result()
            if err is not None:
                print(f"✗ {desc} — {err}")
                failures.append((desc, err))
                continue
            for p in written or []:
                print(f"✓ {desc} → {p}")
                results.append(p)

    if failures:
        print(f"[download_files] Completed with {len(failures)} failure(s).")
    else:
        print(f"[download_files] Done. Wrote {len(results)} file(s) to {data_dir.resolve()}")

    return results


# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Download GEE exports from GCS to local disk.")
    ap.add_argument("--json", required=True, help="Path to export_dst.json written by your exporter")
    ap.add_argument("--out", required=True, help="Local output directory")
    ap.add_argument("--regex", default=None, help="Filter by description regex (e.g., 'landsat_ndvi_.*_2024')")
    ap.add_argument("--workers", type=int, default=8, help="Parallel downloads (default: 8)")
    ap.add_argument("--mirror", action="store_true", help="Mirror bucket subpaths locally")
    ap.add_argument("--creds", default=None, help="Path to service account key JSON (optional; ADC otherwise)")
    ap.add_argument("--project", default=None, help="GCP project for ADC (e.g., ee-hasanresearch)")
    ap.add_argument("--timeout", type=float, default=600.0, help="Per-file timeout (s)")
    ap.add_argument("--retries", type=int, default=2, help="Retry NotFound (race with EE completion)")
    ap.add_argument("--retry-sleep", type=float, default=3.0, help="Seconds to sleep between retries")
    ap.add_argument("--chunk-mb", type=int, default=8, help="Download chunk size (MB)")
    args = ap.parse_args()

    download_files(
        data_dir=args.out,
        tasks=args.json,
        overwrite=True,
        workers=args.workers,
        mirror_bucket_path=args.mirror,
        name_filter_regex=args.regex,
        credentials_json=args.creds,
        project=args.project,
        timeout=args.timeout,
        retries=args.retries,
        retry_sleep=args.retry_sleep,
        chunk_mb=args.chunk_mb,
    )


if __name__ == "__main__":
    main()
