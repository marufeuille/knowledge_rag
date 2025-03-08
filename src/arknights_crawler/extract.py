#!/usr/bin/env python3
"""
Extract script for CloudRun jobs with assets stored on GCS.

This script performs data preprocessing on the crawled HTML data.
It processes the HTML files obtained by the crawler (located by default in "output/original_html"),
extracts the inner HTML of the <div id="body"> element (keeping its HTML tags intact), and saves
the extracted portion as new HTML files. The extracted files are saved locally in "output/html" and then
uploaded to GCS under the same "output/html" folder.

Additionally, for each extraction, metadata is recorded including:
  - id: the extraction id (new sequential id)
  - original_html_id: the id from the crawler (extracted from the original filename, assumed numeric)
  - processed_at: the timestamp when the extraction was performed

Once a specified number (chunk size) of new extractions is accumulated, the metadata records
are saved in a JSON Lines file named as: meta/index_{YYYYMMDDhhmmss}_{chunkNum}.jsonl and uploaded to GCS.
Each such meta file thus represents the cumulative new extraction records in that chunk.

Usage:
    python extract.py [--input-dir INPUT_DIR] [--meta-chunk-size N] [--gcs-project PROJECT]

Environment Variables:
  - GCS_BUCKET_NAME: Name of the GCS bucket (required).
  - GCS_EXTRACTED_PREFIX: GCS folder prefix for extracted HTML files (default: "extracted/").
    (Note: Although the local output directory is "output/html", extraction upload to GCS will use this prefix
     if desired; adjust as needed.)
  - GCS_META_PREFIX: GCS folder prefix for meta JSONL files (default: "meta/").
  - GCS_PROJECT_ID: (Optional) GCS project id to be used if not provided via --gcs-project.

If no input directory is provided, it defaults to "output/original_html".
"""

import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from bs4 import BeautifulSoup
from google.cloud import storage

# Local meta file to store cumulative meta records
LOCAL_META_FILE: Path = Path("/tmp/last_extracted_meta.jsonl")

# GCS configurations from environment variables
GCS_BUCKET_NAME: str = os.environ.get("GCS_BUCKET_NAME", "")
GCS_META_PREFIX: str = os.environ.get("GCS_META_PREFIX", "meta/")


def get_storage_client(project: str | None = None) -> storage.Client:
    """Creates and returns a GCS storage client using the specified project or the environment variable GCS_PROJECT_ID."""
    project = project or os.environ.get("GCS_PROJECT_ID", None)
    if project:
        return storage.Client(project=project)
    return storage.Client()


def upload_to_gcs(
    bucket_name: str,
    source_file: str,
    destination_blob: str,
    project: str | None = None,
) -> bool:
    """Uploads a local file to GCS."""
    try:
        client = get_storage_client(project)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(source_file)
        print(f"Uploaded {source_file} to GCS as {destination_blob}")
        return True
    except Exception as ex:
        print(f"Error uploading {source_file} to GCS: {ex}")
        return False


def extract_body(html_content: str) -> str:
    """
    Extracts the inner HTML of the <div id="body"> element from the provided HTML content.
    Returns the inner HTML (including tags) if found, otherwise an empty string.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    body_div = soup.find("div", id="body")
    if body_div:
        return "".join(str(child) for child in body_div.contents)
    else:
        print("No <div id='body'> found in the HTML.")
        return ""


def load_previous_meta() -> List[Dict]:
    """
    Loads previous extraction meta records from the local meta file, if it exists.
    Each record is a dict with keys: id, original_html_id, processed_at.
    """
    records: List[Dict] = []
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
    Each record output includes: id, original_html_id, processed_at.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    output_path = Path(f"/tmp/meta/index_{timestamp}_{chunk_num}.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for record in meta_records:
            out_record = {
                "id": record["id"],
                "original_html_id": record["original_html_id"],
                "processed_at": record["processed_at"],
            }
            f.write(json.dumps(out_record) + "\n")
    return output_path


def process_extractions(
    input_dir: Path,
    output_dir: Path,
    meta_output_dir: Path,
    meta_chunk_size: int,
    project: str,
) -> None:
    """
    Processes HTML files in the input directory:
      - For each HTML file, extracts the content of <div id="body">.
      - Saves the extracted content locally as output/html/{extraction_id}.html.
      - Creates a meta record with keys: id (extraction id), original_html_id (from filename), processed_at.
      - As soon as the number of new extraction records reaches the chunk size,
        writes these records into a meta JSONL file and uploads it to GCS.
      - Each extracted HTML file is also uploaded to GCS under extracted/{extraction_id}.html.
      - All extracted files and meta files are retained locally.
    """

    html_files = sorted(input_dir.glob("*.html"), key=lambda x: int(x.stem))
    print(f"Found {len(html_files)} original HTML files in {input_dir}.")

    new_meta_records: List[Dict] = []
    cumulative_meta = load_previous_meta()
    next_extraction_id = 1
    if cumulative_meta:
        try:
            max_id = max(record["id"] for record in cumulative_meta)
            next_extraction_id = max_id + 1
        except Exception:
            pass
    chunk_counter = 0
    for file in html_files:
        try:
            original_html_id = int(file.stem)
        except Exception:
            print(f"Skipping non-numeric file: {file.name}")
            continue
        with open(file, "r", encoding="utf-8") as f:
            html_content = f.read()
        extracted_content = extract_body(html_content)
        if not extracted_content:
            print(f"No content extracted from {file.name}. Skipping.")
            continue
        extracted_filename = f"{next_extraction_id}.html"
        local_extracted_path = output_dir / extracted_filename
        with open(local_extracted_path, "w", encoding="utf-8") as f:
            f.write(extracted_content)
        print(f"Extracted content from {file.name} saved as {extracted_filename}.")
        if GCS_BUCKET_NAME:
            destination_blob = os.path.join(output_dir, extracted_filename)
            upload_to_gcs(
                GCS_BUCKET_NAME, str(local_extracted_path), destination_blob, project
            )
        processed_at = datetime.now(timezone.utc).isoformat()
        meta_record = {
            "id": next_extraction_id,
            "original_html_id": original_html_id,
            "processed_at": processed_at,
        }
        new_meta_records.append(meta_record)
        cumulative_meta.append(meta_record)
        next_extraction_id += 1
        if len(new_meta_records) >= meta_chunk_size:
            chunk_counter += 1
            meta_chunk_path = save_meta_chunk(new_meta_records, chunk_counter)
            if GCS_BUCKET_NAME:
                destination_blob = os.path.join(GCS_META_PREFIX, meta_chunk_path.name)
                upload_to_gcs(
                    GCS_BUCKET_NAME, str(meta_chunk_path), destination_blob, project
                )
            new_meta_records.clear()
            with open(LOCAL_META_FILE, "w", encoding="utf-8") as f:
                for record in cumulative_meta:
                    f.write(json.dumps(record) + "\n")
            print(
                f"Flushed meta chunk {chunk_counter}; total extracted records: {len(cumulative_meta)}."
            )
            time.sleep(1)
    if new_meta_records:
        chunk_counter += 1
        meta_chunk_path = save_meta_chunk(new_meta_records, chunk_counter)
        if GCS_BUCKET_NAME:
            destination_blob = os.path.join(GCS_META_PREFIX, meta_chunk_path.name)
            upload_to_gcs(
                GCS_BUCKET_NAME, str(meta_chunk_path), destination_blob, project
            )
        new_meta_records.clear()
        with open(LOCAL_META_FILE, "w", encoding="utf-8") as f:
            for record in cumulative_meta:
                f.write(json.dumps(record) + "\n")
        print(
            f"Flushed final meta chunk {chunk_counter}; total extracted records: {len(cumulative_meta)}."
        )


def main(
    gcp_project: str,
    local_output_parent_dir: str | None = None,
    input_dir_prefix: str = "output/original_html",
    output_dir_prefix: str = "output/extracted",
    meta_output_dir_prefix: str = "output/meta",
    meta_chunk_size: int = 100,
) -> None:
    if not input_dir_prefix.exists():
        print(f"Input directory {input_dir_prefix} does not exist. Exiting.")
        return
    if not GCS_BUCKET_NAME:
        print("GCS_BUCKET_NAME environment variable is not set. Exiting.")
        return

    d: tempfile.TemporaryDirectory | None = None
    if local_output_parent_dir is None:
        d = tempfile.TemporaryDirectory()
        local_output_parent_dir = d.name
    local_output_parent_dir_path = Path(local_output_parent_dir)
    local_output_parent_dir_path.mkdir(parents=True, exist_ok=True)
    input_dir = Path(
        os.path.join(
            local_output_parent_dir_path,
            input_dir_prefix,
        )
    )
    output_dir = Path(
        os.path.join(
            local_output_parent_dir_path,
            output_dir_prefix,
        )
    )
    meta_output_dir = Path(
        os.path.join(
            local_output_parent_dir_path,
            meta_output_dir_prefix,
        )
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    meta_output_dir.mkdir(parents=True, exist_ok=True)

    try:
        process_extractions(
            input_dir_prefix,
            output_dir_prefix,
            meta_output_dir_prefix,
            meta_chunk_size,
            gcp_project,
        )
    finally:
        if d:
            d.cleanup()


if __name__ == "__main__":
    main()
