#!/usr/bin/env python3
"""
Crawler script modified to integrate with the SQLite index and to use sitemap data.

This script downloads pages referenced in a sitemap XML, rather than using a static list of URLs.
It fetches the sitemap from a provided URL (defaults to "https://arknights.wikiru.jp/index.php?plugin=sitemap"),
parses the XML to extract URLs and their last modification dates, and processes each URL using an SQLite
database (output/index.db) to maintain an index. For each URL, only the file part (i.e., the path and query,
excluding the scheme and domain) is stored in the database as original_filename.

Before downloading a page, the file name (extracted from the URL) is compared against the database.
If a record exists, the script compares the stored created_at timestamp with the sitemap's lastmod value.
If the sitemap's lastmod is more recent than the stored created_at, the page is re-downloaded and the record
is updated. Otherwise, if no record exists, a new record is created and the page is downloaded.

After a successful download, the script sleeps for 3 seconds to avoid overwhelming the server.

The SQLite table "pages" has the following columns:
  - id: auto-increment primary key (INTEGER)
  - original_filename: the file part of the URL (e.g., "/index.php?plugin=sitemap")
  - created_at: timestamp of the last download (ISO format)

Usage:
    python crawler.py [--sitemap-url SITEMAP_URL] [--urls url1 url2 ...]
If --urls is provided, those URLs will be used (with no lastmod information). Otherwise, URLs are fetched from
the sitemap, which defaults to "https://arknights.wikiru.jp/index.php?plugin=sitemap".
"""

import os
import sqlite3
import requests
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

DB_PATH: Path = Path("output/index.db")
DEST_DIR: Path = Path("output/original_html")

def extract_file_name(url: str) -> str:
    """
    Extracts the file part (path and query) from a URL, excluding the scheme and domain.
    
    Args:
        url (str): The full URL.
        
    Returns:
        str: The extracted file part.
    """
    parsed = urlparse(url)
    file_name = parsed.path
    if parsed.query:
        file_name += '?' + parsed.query
    return file_name

def init_db() -> sqlite3.Connection:
    """
    Initializes the SQLite database connection.
    
    Returns:
        sqlite3.Connection: The SQL connection object.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_filename TEXT NOT NULL UNIQUE,
            created_at DATETIME NOT NULL
        )
        """
    )
    conn.commit()
    return conn

def get_registered_record(conn: sqlite3.Connection, file_name: str) -> Optional[Tuple[int, str]]:
    """
    Checks if the file name is already registered in the database.
    
    Args:
        conn (sqlite3.Connection): The SQLite connection.
        file_name (str): The file part of the URL.
        
    Returns:
        Optional[Tuple[int, str]]: Tuple of (id, created_at) if the record exists; None otherwise.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT id, created_at FROM pages WHERE original_filename = ?", (file_name,))
    result = cursor.fetchone()
    return (result[0], result[1]) if result else None

def register_url(conn: sqlite3.Connection, file_name: str) -> int:
    """
    Registers the file name into the database, returning a unique ID.
    
    Args:
        conn (sqlite3.Connection): The SQLite connection.
        file_name (str): The file part of the URL.
    
    Returns:
        int: The auto-generated ID.
    """
    cursor = conn.cursor()
    timestamp = datetime.now().isoformat()
    cursor.execute(
        "INSERT INTO pages (original_filename, created_at) VALUES (?, ?)",
        (file_name, timestamp)
    )
    conn.commit()
    return cursor.lastrowid

def update_record(conn: sqlite3.Connection, record_id: int) -> None:
    """
    Updates the created_at field of an existing record to the current time.
    
    Args:
        conn (sqlite3.Connection): The SQLite connection.
        record_id (int): The ID of the record to update.
    """
    cursor = conn.cursor()
    timestamp = datetime.now().isoformat()
    cursor.execute("UPDATE pages SET created_at = ? WHERE id = ?", (timestamp, record_id))
    conn.commit()

def download_page(url: str) -> Optional[str]:
    """
    Downloads the content of the specified URL.
    
    Args:
        url (str): The URL to download.
        
    Returns:
        Optional[str]: The content of the page if successful; None otherwise.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as ex:
        print(f"Failed to download {url}: {ex}")
        return None

def process_urls(urls: List[Tuple[str, Optional[str]]]) -> None:
    """
    Processes a list of URLs with optional lastmod information:
      - For each URL, extracts the file part and checks if it is already registered.
      - If registered, and a lastmod is provided, compares lastmod with the stored created_at.
        If the lastmod is more recent, re-downloads the page and updates the record.
      - If not registered, registers the URL and downloads the page.
      - Saves the page content as output/original_html/{id}.html.
      - Sleeps for 3 seconds after a successful download.
    
    Args:
        urls (List[Tuple[str, Optional[str]]]): List of tuples (url, lastmod). lastmod may be None.
    """
    DEST_DIR.mkdir(parents=True, exist_ok=True)
    conn = init_db()
    
    for url, lastmod in urls:
        file_name = extract_file_name(url)
        record = get_registered_record(conn, file_name)
        download_needed = False
        
        if record:
            record_id, created_at_str = record
            if lastmod:
                try:
                    sitemap_lastmod = datetime.fromisoformat(lastmod.replace("Z", "+00:00"))
                    record_created = datetime.fromisoformat(created_at_str)
                    if sitemap_lastmod > record_created:
                        print(f"Update needed for {file_name}: sitemap lastmod {lastmod} is newer than record {created_at_str}.")
                        download_needed = True
                    else:
                        print(f"No update for {file_name}: record is up-to-date.")
                except Exception as ex:
                    print(f"Error comparing dates for {file_name}: {ex}. Skipping update.")
            else:
                print(f"{file_name} is already registered with ID {record_id}.")
        else:
            download_needed = True
        
        if download_needed:
            if not record:
                record_id = register_url(conn, file_name)
                print(f"Registered {file_name} with new ID {record_id}")
            else:
                print(f"Re-downloading {file_name} for update (ID {record_id})")
            
            content = download_page(url)
            if content is None:
                print(f"Skipping {url} due to download failure.")
                continue
            
            file_path = DEST_DIR / f"{record_id}.html"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            update_record(conn, record_id)
            print(f"Saved content of {url} as {file_path.name}")
            time.sleep(3)  # Sleep for 3 seconds after successful download
    
    conn.close()

def get_sitemap_urls(sitemap_url: str) -> List[Tuple[str, Optional[str]]]:
    """
    Fetches the sitemap XML from the provided URL and extracts URLs along with their lastmod values.
    
    Args:
        sitemap_url (str): The URL of the sitemap XML.
    
    Returns:
        List[Tuple[str, Optional[str]]]: List of tuples (url, lastmod). lastmod may be None.
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
                lastmod = lastmod_element.text.strip() if (lastmod_element is not None and lastmod_element.text) else None
                urls.append((loc, lastmod))
        return urls
    except Exception as ex:
        print(f"Failed to parse sitemap XML: {ex}")
        return []

def parse_arguments() -> List[Tuple[str, Optional[str]]]:
    """
    Parses command line arguments to retrieve URLs.
    If --urls is provided, returns them as tuples with lastmod set to None.
    Otherwise, fetches URLs from the sitemap (defaults to "https://arknights.wikiru.jp/index.php?plugin=sitemap").
    
    Returns:
        List[Tuple[str, Optional[str]]]: List of tuples (url, lastmod).
    """
    parser = argparse.ArgumentParser(description="Download pages using SQLite for indexing with sitemap support.")
    parser.add_argument(
        "--sitemap-url",
        type=str,
        default="https://arknights.wikiru.jp/index.php?plugin=sitemap",
        help="URL of the sitemap XML. Defaults to 'https://arknights.wikiru.jp/index.php?plugin=sitemap'."
    )
    parser.add_argument(
        "--urls",
        nargs="*",
        help="List of URLs to process. If provided, these URLs will be used with no lastmod info."
    )
    args = parser.parse_args()
    if args.urls and len(args.urls) > 0:
        return [(url, None) for url in args.urls]
    return get_sitemap_urls(args.sitemap_url)

if __name__ == "__main__":
    urls_to_crawl = parse_arguments()
    if not urls_to_crawl:
        print("No URLs found to process.")
    else:
        process_urls(urls_to_crawl)
