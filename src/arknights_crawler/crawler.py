#!/usr/bin/env python3
"""
Crawler script for CloudRun jobs with assets stored on GCS.

This script downloads pages referenced in a sitemap XML (default:
"https://arknights.wikiru.jp/index.php?plugin=sitemap") and processes only those
that have been updated since the last fetch. If the same URL appears with a newer
lastmod than any previously fetched record, a new record (with a new id) is created.
The output consists of:

1. HTML files – each downloaded page is saved locally as {id}.html (an id is used
   because the original URL is too long for a file name). Each HTML file is immediately
   uploaded to GCS.
2. Meta information in JSON Lines format – each record contains:
      id, filename, fetched_at.
   These records (which also include the original URL internally for update comparisons)
   are collected and, once a specified number (chunk size) of new pages is reached,
   saved in a meta JSONL file named as: meta/index_{YYYYMMDDhhmmss}_{chunkNum}.jsonl.
   This file is then uploaded to GCS. Each such meta file thus represents the cumulative
   new records processed in that chunk.

Determination of updated pages:
  - The sitemap provides a lastmod timestamp for each URL.
  - This script downloads the latest meta file (if available) from GCS (from the prefix specified by GCS_META_PREFIX)
    and loads records (each includes "url" and "fetched_at").
  - For each URL in the current sitemap, if a previous record exists for that URL with
    fetched_at >= sitemap.lastmod, it is skipped; otherwise, the page is fetched and a new record is created.
  - Even if the same URL appears again with an update, a new record is created so that the latest data
    can be determined by the combination of URL and fetched_at.

Output and upload:
  - HTML files are individually uploaded to GCS under the path specified by GCS_HTML_PREFIX.
  - Meta JSONL files are written in chunks (chunk size configurable via --meta-chunk-size, default 10)
    and uploaded under GCS_META_PREFIX.
  
Environment Variables:
  - GCS_BUCKET_NAME: Name of the GCS bucket (required).
  - GCS_HTML_PREFIX: GCS folder prefix for HTML files (default: "html/").
  - GCS_META_PREFIX: GCS folder prefix for meta JSONL files (default: "meta/").

Additionally, the GCS project name can be specified via the --gcs-project argument.

Usage:
    python crawler.py [--sitemap-url SITEMAP_URL] [--meta-chunk-size N] [--gcs-project PROJECT]
If no sitemap URL is provided, the default is used.
"""

import os
import requests
import argparse
import time
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple, Dict
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
import re

from google.cloud import storage

# Configuration for local temporary storage
LOCAL_HTML_DIR: Path = Path("/tmp/html")
LOCAL_META_FILE: Path = Path("/tmp/latest_meta.jsonl")

# GCS configurations from environment variables
GCS_BUCKET_NAME: str = os.environ.get("GCS_BUCKET_NAME", "")
GCS_HTML_PREFIX: str = os.environ.get("GCS_HTML_PREFIX", "html/")
GCS_META_PREFIX: str = os.environ.get("GCS_META_PREFIX", "meta/")

def get_storage_client(project: Optional[str] = None) -> storage.Client:
    """Creates and returns a GCS storage client, using the specified project if provided."""
    if project:
        return storage.Client(project=project)
    return storage.Client()

def download_from_gcs(bucket_name: str, source_blob_name: str, destination_file_name: str, project: Optional[str] = None) -> bool:
    """Downloads a blob from GCS to a local file."""
    try:
        client = get_storage_client(project)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        if blob.exists():
            blob.download_to_filename(destination_file_name)
            print(f"Downloaded {source_blob_name} from GCS to {destination_file_name}")
            return True
        else:
            print(f"Blob {source_blob_name} does not exist in bucket {bucket_name}.")
            return False
    except Exception as ex:
        print(f"Error downloading {source_blob_name} from GCS: {ex}")
        return False

def upload_to_gcs(bucket_name: str, source_file_name: str, destination_blob_name: str, project: Optional[str] = None) -> bool:
    """Uploads a file to GCS."""
    try:
        client = get_storage_client(project)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"Uploaded {source_file_name} to GCS as {destination_blob_name}")
        return True
    except Exception as ex:
        print(f"Error uploading {source_file_name} to GCS: {ex}")
        return False

def extract_file_name(url: str) -> str:
    """
    Extracts the file part (path and query) from a URL, excluding the scheme and domain.
    """
    parsed = urlparse(url)
    file_name = parsed.path
    if parsed.query:
        file_name += '?' + parsed.query
    return file_name

def parse_sitemap(sitemap_url: str) -> List[Tuple[str, Optional[str]]]:
    """
    Fetches and parses the sitemap XML, returning a list of tuples: (url, lastmod).
    """
    try:
        response = requests.get(sitemap_url, timeout=10)
        response.raise_for_status()
        xml_content = response.text
    except Exception as ex:
        print(f"Failed to download sitemap from {sitemap_url}: {ex}")
        return []
    try:
        root = ET.fromstring(xml_content)
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = []
        for url_element in root.findall("ns:url", namespace):
            loc_element = url_element.find("ns:loc", namespace)
            lastmod_element = url_element.find("ns:lastmod", namespace)
            if loc_element is not None and loc_element.text:
                loc = loc_element.text.strip()
                lastmod = (lastmod_element.text.strip() if lastmod_element is not None and lastmod_element.text else None)
                urls.append((loc, lastmod))
        return urls
    except Exception as ex:
        print(f"Failed to parse sitemap XML: {ex}")
        return []

def load_previous_meta(project: Optional[str] = None) -> List[Dict]:
    """
    Loads previous meta records from the latest meta file in GCS under the prefix GCS_META_PREFIX.
    It lists blobs in the bucket with that prefix, selects the one with the latest timestamp
    (extracted from its filename), downloads it, and loads its JSONL records.
    Each record contains: id, filename, fetched_at, url.
    """
    records = []
    LOCAL_META_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        client = get_storage_client(project)
        bucket = client.get_bucket(GCS_BUCKET_NAME)
        blobs = list(bucket.list_blobs(prefix=GCS_META_PREFIX))
        latest_blob = None
        latest_ts = None
        pattern = re.compile(r'index_(\d{14})')
        for blob in blobs:
            m = pattern.search(blob.name)
            if m:
                ts_str = m.group(1)
                ts = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
                if (latest_ts is None) or (ts > latest_ts):
                    latest_ts = ts
                    latest_blob = blob
        if latest_blob:
            print(f"Latest meta file in GCS: {latest_blob.name}")
            latest_blob.download_to_filename(str(LOCAL_META_FILE))
            print(f"Downloaded latest meta file {latest_blob.name} to {LOCAL_META_FILE}")
        else:
            print("No meta file found in GCS under the specified prefix.")
    except Exception as e:
        print(f"Error loading meta files from GCS: {e}")
        return records
    if LOCAL_META_FILE.exists():
        with open(LOCAL_META_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    records.append(record)
                except Exception as ex:
                    print(f"Error parsing meta record: {ex}")
    return records

def save_meta_chunk(meta_records: List[Dict], chunk_num: int) -> Path:
    """
    Writes the provided meta_records as a single JSONL file.
    The file is named as: meta/index_{timestamp}_{chunkNum}.jsonl, where timestamp is in YYYYMMDDhhmmss format.
    Returns the path to the generated file.
    Each record output includes: id, filename, fetched_at.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    output_path = Path(f"/tmp/meta/index_{timestamp}_{chunk_num}.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for record in meta_records:
            out_record = {
                "id": record["id"],
                "filename": record["filename"],
                "fetched_at": record["fetched_at"]
            }
            f.write(json.dumps(out_record) + "\n")
    return output_path

def download_page(url: str) -> Optional[str]:
    """
    Downloads the content of the specified URL.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as ex:
        print(f"Failed to download {url}: {ex}")
        return None

def process_updates(sitemap_url: str, meta_chunk_size: int, project: Optional[str] = None) -> None:
    """
    Processes the sitemap, fetches pages that are updated relative to previous meta data,
    outputs each HTML file with a new id, and creates meta records.
    HTML files and new meta records are immediately uploaded to GCS.
    
    This function accumulates new meta records. Once the number of new records reaches the chunk size,
    it flushes them: saving them as a meta JSONL file (named meta/index_{timestamp}_{chunkNum}.jsonl) and uploading it to GCS.
    After processing all URLs, if any new records remain (less than the chunk size), they are also flushed.
    
    For each URL in the sitemap:
      - If a previous record exists with fetched_at >= sitemap.lastmod, skip.
      - Otherwise, download the page and create a new record.
    """
    LOCAL_HTML_DIR.mkdir(parents=True, exist_ok=True)
    
    sitemap_entries = parse_sitemap(sitemap_url)
    print(f"Found {len(sitemap_entries)} URLs in sitemap.")
    
    previous_meta = load_previous_meta(project)
    # Build lookup: url -> latest fetched_at from previous meta.
    url_latest: Dict[str, datetime] = {}
    for record in previous_meta:
        try:
            rec_url = record["url"]
            fetched_at = datetime.fromisoformat(record["fetched_at"])
            if rec_url in url_latest:
                if fetched_at > url_latest[rec_url]:
                    url_latest[rec_url] = fetched_at
            else:
                url_latest[rec_url] = fetched_at
        except Exception as ex:
            print(f"Error processing previous meta record: {ex}")
    
    new_meta_records: List[Dict] = []
    cumulative_meta = previous_meta.copy()
    next_id = 1
    if previous_meta:
        try:
            max_id = max(record["id"] for record in previous_meta)
            next_id = max_id + 1
        except Exception:
            pass

    chunk_counter = 0
    for url, lastmod in sitemap_entries:
        fetch_required = False
        try:
            if lastmod:
                sitemap_lastmod = datetime.fromisoformat(lastmod.replace("Z", "+00:00"))
            else:
                sitemap_lastmod = None
        except Exception as ex:
            print(f"Error parsing lastmod for {url}: {ex}")
            sitemap_lastmod = None

        if url in url_latest and sitemap_lastmod is not None:
            if sitemap_lastmod > url_latest[url]:
                print(f"Update detected for {url}.")
                fetch_required = True
            else:
                print(f"No update for {url}. Skipping.")
        else:
            fetch_required = True

        if fetch_required:
            content = download_page(url)
            if content is None:
                print(f"Failed to download {url}.")
                continue
            html_filename = f"{next_id}.html"
            local_html_path = LOCAL_HTML_DIR / html_filename
            with open(local_html_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Saved HTML for {url} as {html_filename}.")
            if GCS_BUCKET_NAME:
                destination_blob = os.path.join(GCS_HTML_PREFIX, html_filename)
                upload_to_gcs(GCS_BUCKET_NAME, str(local_html_path), destination_blob, project)
            fetched_at = datetime.now(timezone.utc).isoformat()
            new_record = {
                "id": next_id,
                "filename": html_filename,
                "fetched_at": fetched_at,
                "url": url
            }
            new_meta_records.append(new_record)
            cumulative_meta.append(new_record)
            next_id += 1
            time.sleep(3)
            # If new_meta_records reached the chunk size, flush them.
            if len(new_meta_records) >= meta_chunk_size:
                chunk_counter += 1
                meta_chunk_path = save_meta_chunk(new_meta_records, chunk_counter)
                if GCS_BUCKET_NAME:
                    destination_blob = os.path.join(GCS_META_PREFIX, meta_chunk_path.name)
                    upload_to_gcs(GCS_BUCKET_NAME, str(meta_chunk_path), destination_blob, project)
                # Clear the new records for new chunk.
                new_meta_records.clear()
                # Update LOCAL_META_FILE with cumulative meta.
                with open(LOCAL_META_FILE, "w", encoding="utf-8") as f:
                    for record in cumulative_meta:
                        f.write(json.dumps(record) + "\n")
                print(f"Flushed meta chunk {chunk_counter}; cumulative meta records: {len(cumulative_meta)}.")

    # Flush any remaining new meta records if they did not reach chunk_size.
    if new_meta_records:
        chunk_counter += 1
        meta_chunk_path = save_meta_chunk(new_meta_records, chunk_counter)
        if GCS_BUCKET_NAME:
            destination_blob = os.path.join(GCS_META_PREFIX, meta_chunk_path.name)
            upload_to_gcs(GCS_BUCKET_NAME, str(meta_chunk_path), destination_blob, project)
        new_meta_records.clear()
        with open(LOCAL_META_FILE, "w", encoding="utf-8") as f:
            for record in cumulative_meta:
                f.write(json.dumps(record) + "\n")
        print(f"Flushed final meta chunk {chunk_counter}; total meta records: {len(cumulative_meta)}.")

def save_meta_chunk(meta_records: List[Dict], chunk_num: int) -> Path:
    """
    Writes the provided meta_records as a single JSONL file.
    The file is named as: meta/index_{YYYYMMDDhhmmss}_{chunkNum}.jsonl.
    Returns the path to the generated file.
    Each record output includes: id, filename, fetched_at.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    output_path = Path(f"/tmp/meta/index_{timestamp}_{chunk_num}.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for record in meta_records:
            out_record = {
                "id": record["id"],
                "filename": record["filename"],
                "fetched_at": record["fetched_at"]
            }
            f.write(json.dumps(out_record) + "\n")
    return output_path

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl sitemap and fetch updated pages, outputting HTML and meta JSONL files to GCS.")
    parser.add_argument("--sitemap-url", type=str, default="https://arknights.wikiru.jp/index.php?plugin=sitemap",
                        help="URL of the sitemap XML. Defaults to https://arknights.wikiru.jp/index.php?plugin=sitemap")
    parser.add_argument("--meta-chunk-size", type=int, default=10,
                        help="Number of new meta records per JSONL chunk file. Default is 10.")
    parser.add_argument("--gcs-project", type=str, default="",
                        help="GCS project name. Optional if set via environment variable.")
    return parser.parse_args()

def main() -> None:
    args = parse_arguments()
    project = args.gcs_project if args.gcs_project else None
    if not GCS_BUCKET_NAME:
        print("GCS_BUCKET_NAME environment variable is not set. Exiting.")
    else:
        process_updates(args.sitemap_url, args.meta_chunk_size, project)

if __name__ == "__main__":
    main()
