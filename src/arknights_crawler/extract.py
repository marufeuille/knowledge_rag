#!/usr/bin/env python3
"""
Extract module for processing crawled HTML files from Google Cloud Storage.

This module connects to Google Cloud Storage to:
  1. Retrieve the latest metadata file from the meta/ directory in the bucket and determine the
     maximum processed metadata IDs.
  2. List HTML files in the html/ folder whose original_html_id is greater than the maximum
     already processed.
  3. Download each new HTML file, extract the content within the <div id="body"> tag,
     and upload the extracted HTML to output/extracted/.
  4. Accumulate metadata for each processed file and, in chunks, upload JSONL files to
     output/meta/.

The design is implemented with pure functions for business logic and isolates I/O operations.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple

from bs4 import BeautifulSoup
from google.cloud import storage


# ---------------------------
# Pure functions for HTML extraction
# ---------------------------
def extract_body(html: str) -> str:
    """Extract HTML content within the div having id 'body'.

    This function uses BeautifulSoup to parse the given HTML text and extract the
    inner HTML of the <div id="body"> element. The outer div tag is excluded, only
    its children are returned.

    Args:
        html (str): The input HTML content as a string.

    Returns:
        str: The inner HTML of the div with id 'body'. Returns an empty string if not found.
    """
    soup = BeautifulSoup(html, "html.parser")
    body_div = soup.find("div", id="body")
    if body_div:
        return "".join(str(child) for child in body_div.children)
    return ""


def process_html_content(
    original_html_id: str, html_content: str, new_id: int
) -> Tuple[str, Dict[str, Any]]:
    """Process HTML content to extract target segment and create metadata.

    Extracts content using extract_body and compiles metadata including a new auto-assigned
    ID, the original HTML ID derived from GCS blob name, and the current processing timestamp.

    Args:
        original_html_id (str): The original identifier derived from the HTML file name.
        html_content (str): The HTML content as a string.
        new_id (int): The new auto-assigned ID for the extraction.

    Returns:
        Tuple[str, Dict[str, Any]]: A tuple with the extracted HTML content and its metadata.
    """
    extracted_html = extract_body(html_content)
    processed_at = datetime.now(datetime.timezone.utc).isoformat() + "Z"
    meta = {
        "id": new_id,
        "original_html_id": original_html_id,
        "processed_at": processed_at,
    }
    return extracted_html, meta


# ---------------------------
# GCS Helper Functions
# ---------------------------
def get_latest_max_ids(
    bucket: storage.bucket.Bucket, meta_prefix: str = "meta/index_"
) -> Tuple[int, int]:
    """Retrieve the maximum processed id and maximum original_html_id from the latest metadata file in GCS.

    It lists all blobs under the given meta_prefix, selects the one with the highest timestamp based
    on its filename, reads its JSONL content, and finds the maximum values.
    Assumes that both 'id' and 'original_html_id' can be interpreted as integers.

    Args:
        bucket (storage.bucket.Bucket): The GCS bucket object.
        meta_prefix (str): The prefix in the bucket where metadata files are stored.

    Returns:
        Tuple[int, int]: A tuple (max_meta_id, max_original_html_id). Returns (0, 0) if no metadata file exists.
    """
    blobs = list(bucket.list_blobs(prefix=meta_prefix))
    if not blobs:
        return 0, 0

    # Select the blob with the highest (latest) timestamp in its filename.
    latest_blob = max(
        blobs,
        key=lambda b: b.name.split("_")[-1].split(".")[0] if "_" in b.name else "",
    )

    try:
        content = latest_blob.download_as_text(encoding="utf-8")
    except Exception:
        return 0, 0

    max_meta_id = 0
    max_original_id = 0
    for line in content.splitlines():
        try:
            record = json.loads(line)
            record_id = int(record.get("id", 0))
            original_id = int(record.get("original_html_id", 0))
            if record_id > max_meta_id:
                max_meta_id = record_id
            if original_id > max_original_id:
                max_original_id = original_id
        except (ValueError, json.JSONDecodeError):
            continue
    return max_meta_id, max_original_id


def list_new_html_blobs(
    bucket: storage.bucket.Bucket, html_prefix: str = "html/", min_original_id: int = 0
) -> List[storage.Blob]:
    """List HTML blobs in the bucket with original_html_id greater than min_original_id.

    Assumes that HTML files are stored under html_prefix and each file name (without extension)
    represents the original_html_id, which can be converted to an integer.

    Args:
        bucket (storage.bucket.Bucket): The GCS bucket object.
        html_prefix (str): The prefix where HTML files are stored.
        min_original_id (int): The minimum original_html_id that has been processed.

    Returns:
        List[storage.Blob]: A list of GCS Blob objects for new HTML files to process.
    """
    new_blobs = []
    blobs = bucket.list_blobs(prefix=html_prefix)
    for blob in blobs:
        base_name = os.path.basename(blob.name)
        original_id_str, ext = os.path.splitext(base_name)
        try:
            original_id = int(original_id_str)
            if original_id > min_original_id:
                new_blobs.append(blob)
        except ValueError:
            continue
    new_blobs.sort(key=lambda b: int(os.path.splitext(os.path.basename(b.name))[0]))
    return new_blobs


def upload_string_to_gcs(
    bucket: storage.bucket.Bucket,
    destination_blob_name: str,
    content: str,
    content_type: str,
) -> None:
    """Upload a string content to GCS at the specified blob destination.

    Args:
        bucket (storage.bucket.Bucket): The GCS bucket object.
        destination_blob_name (str): The destination path in the bucket.
        content (str): The content to upload.
        content_type (str): The MIME type of the content.
    """
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(content, content_type=content_type)


# ---------------------------
# Main extraction process using GCS
# ---------------------------
def run_extraction(
    input_dir: str, meta_chunk_size: int, gcs_project: str, gcs_bucket_name: str
) -> None:
    """Run the extraction process by interfacing with Google Cloud Storage.

    1. Connects to GCS using provided project and bucket names.
    2. Retrieves the latest metadata file to determine the maximum processed IDs.
    3. Lists new HTML files in the 'html/' folder with original_html_id greater than the maximum processed.
    4. For each new HTML file:
         - Downloads the file content.
         - Extracts the target HTML segment.
         - Uploads the extracted HTML to 'output/extracted/{new_id}.html'.
         - Accumulates metadata.
    5. Uploads metadata in chunks to 'output/meta/' as JSONL files.

    Args:
        input_dir (str): Not used; provided for compatibility with CLI wrapper.
        meta_chunk_size (int): Number of metadata records per output JSONL file.
        gcs_project (str): Google Cloud Storage project name.
        gcs_bucket_name (str): Google Cloud Storage bucket name.
    """
    client = storage.Client(project=gcs_project)
    bucket = client.bucket(gcs_bucket_name)

    # Retrieve maximum IDs from the latest metadata file
    max_meta_id, max_original_id = get_latest_max_ids(
        bucket,
        meta_prefix="meta/index_",
    )
    new_id = max_meta_id + 1

    # List new HTML blobs from 'html/' folder
    new_html_blobs = list_new_html_blobs(
        bucket, html_prefix="html/", min_original_id=max_original_id
    )

    meta_records: List[Dict[str, Any]] = []

    for blob in new_html_blobs:
        base_name = os.path.basename(blob.name)
        original_html_id, _ = os.path.splitext(base_name)
        try:
            html_content = blob.download_as_text(encoding="utf-8")
        except Exception:
            continue

        extracted_html, meta = process_html_content(
            original_html_id, html_content, new_id
        )

        # Upload extracted HTML to GCS: output/extracted/{new_id}.html
        extracted_blob_name = f"extracted/{new_id}.html"
        upload_string_to_gcs(
            bucket, extracted_blob_name, extracted_html, content_type="text/html"
        )

        meta_records.append(meta)
        new_id += 1

        if len(meta_records) >= meta_chunk_size:
            timestamp = datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
            meta_blob_name = f"meta/extracted_{timestamp}.jsonl"
            meta_content = "\n".join(
                json.dumps(record, ensure_ascii=False) for record in meta_records
            )
            upload_string_to_gcs(
                bucket, meta_blob_name, meta_content, content_type="application/json"
            )
            meta_records = []

    if meta_records:
        timestamp = datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
        meta_blob_name = f"meta/extracted_{timestamp}.jsonl"
        meta_content = "\n".join(
            json.dumps(record, ensure_ascii=False) for record in meta_records
        )
        upload_string_to_gcs(
            bucket, meta_blob_name, meta_content, content_type="application/json"
        )


def main(
    input_dir: str, meta_chunk_size: int, gcs_project: str, gcs_bucket_name: str
) -> None:
    """Main entry point for the extraction process from GCS.

    This function receives parameters (for future CLI integration) and triggers the
    extraction process by invoking run_extraction.

    Args:
        input_dir (str): Provided for CLI compatibility (unused).
        meta_chunk_size (int): Number of metadata records per JSONL file.
        gcs_project (str): Google Cloud Storage project name.
        gcs_bucket_name (str): Google Cloud Storage bucket name.
    """
    run_extraction(input_dir, meta_chunk_size, gcs_project, gcs_bucket_name)


if __name__ == "__main__":
    # For testing purposes, sample parameters are defined below.
    sample_input_dir = "unused_input_dir"  # Not used in GCS processing
    sample_meta_chunk_size = 10  # Example metadata chunk size
    sample_gcs_project = "my-sample-lab"
    sample_gcs_bucket_name = "arknights-rag"
    main(
        sample_input_dir,
        sample_meta_chunk_size,
        sample_gcs_project,
        sample_gcs_bucket_name,
    )
