#!/usr/bin/env python3
import sys
import argparse

from arknights_crawler import crawler, extract

def main():
    parser = argparse.ArgumentParser(
        description="Common entry point for crawler and extraction commands."
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Sub-commands")
    
    # Subparser for the 'crawl' command
    crawl_parser = subparsers.add_parser("crawl", help="Run crawler functionality.")
    crawl_parser.add_argument("--sitemap-url", type=str, default="https://arknights.wikiru.jp/index.php?plugin=sitemap",
                              help="URL of the sitemap XML (default is https://arknights.wikiru.jp/index.php?plugin=sitemap)")
    crawl_parser.add_argument("--meta-chunk-size", type=int, default=10,
                              help="Number of meta records per chunk (default is 10)")
    crawl_parser.add_argument("--gcs-project", type=str, default="",
                              help="Google Cloud Storage project name")

    # Subparser for the 'extract' command
    extract_parser = subparsers.add_parser("extract", help="Run extraction functionality.")
    extract_parser.add_argument("--input-dir", type=str, default="output/original_html",
                                help="Directory of crawled HTML files (default is output/original_html)")
    extract_parser.add_argument("--meta-chunk-size", type=int, default=5,
                                help="Number of meta extraction records per chunk (default is 5)")
    extract_parser.add_argument("--gcs-project", type=str, default="",
                                help="Google Cloud Storage project name")

    args = parser.parse_args()

    if args.command == "crawl":
        # Call the main function from crawler.py
        crawler.main()
    elif args.command == "extract":
        # Call the main function from extract.py
        extract.main()
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
