#!/usr/bin/env python3
"""
GITEX Global 2025 Exhibitor Scraper
Fetches exhibitor details (name, country, sector) from https://exhibitors.gitex.com/gitex-global-2025/Exhibitor
"""

import csv
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://exhibitors.gitex.com"
EVENT_SLUG = "gitex-global-2025"
FETCH_URL = f"{BASE_URL}/{EVENT_SLUG}/Exhibitor/fetchExhibitors"
PAGE_LIMIT = 100  # Records per API request


def fetch_exhibitors_page(start: int) -> str:
    """Fetch a page of exhibitors from the API."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "*/*",
        "Origin": BASE_URL,
        "Referer": f"{BASE_URL}/{EVENT_SLUG}/Exhibitor",
    }
    data = {
        "limit": PAGE_LIMIT,
        "start": start,
        "keyword_search": "",
        "cuntryId": "",
        "event_prod_cat_id": "",
        "exb_listed_as": "",
        "InitialKey": "",
        "selected_event_id": "",
        "start_up_exhibitors": "",
        "pav_country_id": "",
        "type": "",
        "vacancies": "",
        "product_search": "",
        "new_category": "",
        "new_sub_category": "",
        "new_sub_sub_category": "",
        "search_by_venue": "",
        "event_sector_value": "",
    }
    response = requests.post(FETCH_URL, headers=headers, data=data, timeout=30)
    response.raise_for_status()
    return response.text


def parse_exhibitor_card(card) -> dict | None:
    """Extract exhibitor name, country, and sectors from an exhibitor card."""
    try:
        # Exhibitor name
        heading = card.select_one("h4.heading")
        name = heading.get_text(strip=True) if heading else ""

        # Country - in span with font-weight in the second p of .web
        web_div = card.select_one("div.web")
        country = ""
        if web_div:
            paragraphs = web_div.find_all("p", limit=3)
            for p in paragraphs:
                span = p.find("span", style=re.compile(r"font-weight:\s*600"))
                if span:
                    country = span.get_text(strip=True)
                    break

        # Sectors - from ul.sector_block li
        sector_list = card.select("ul.sector_block li")
        sectors = [li.get_text(strip=True) for li in sector_list if li.get_text(strip=True)]
        sector_str = "; ".join(sectors) if sectors else ""

        return {
            "exhibitor_name": name,
            "country": country,
            "sector": sector_str,
        }
    except Exception:
        return None


def scrape_all_exhibitors(max_exhibitors: int = 0) -> list[dict]:
    """Scrape all exhibitors from GITEX Global 2025."""
    exhibitors = []
    start = 0

    while True:
        if max_exhibitors and len(exhibitors) >= max_exhibitors:
            break
        print(f"Fetching exhibitors {start + 1} to {start + PAGE_LIMIT}...")

        html = fetch_exhibitors_page(start)

        # Check for "no more content" message
        if not html.strip() or "No more content available" in html:
            break

        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("div.item.list-group-item")

        if not cards:
            break

        for card in cards:
            if max_exhibitors and len(exhibitors) >= max_exhibitors:
                break
            exhibitor = parse_exhibitor_card(card)
            if exhibitor and exhibitor["exhibitor_name"]:
                exhibitors.append(exhibitor)

        print(f"  Found {len(cards)} exhibitors (total: {len(exhibitors)})")

        if len(cards) < PAGE_LIMIT:
            break

        start += PAGE_LIMIT
        time.sleep(0.5)  # Be polite to the server

    return exhibitors


def save_to_csv(exhibitors: list[dict], output_path: str = "gitex_exhibitors.csv") -> None:
    """Save exhibitors to CSV file."""
    if not exhibitors:
        print("No exhibitors to save.")
        return

    fieldnames = ["exhibitor_name", "country", "sector"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(exhibitors)

    print(f"\nSaved {len(exhibitors)} exhibitors to {output_path}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape GITEX exhibitors")
    parser.add_argument("--max", type=int, default=0, help="Max exhibitors to fetch (0=all)")
    args = parser.parse_args()

    print("GITEX Global 2025 Exhibitor Scraper")
    print("=" * 50)

    exhibitors = scrape_all_exhibitors(max_exhibitors=args.max)

    output_file = Path(__file__).parent / "gitex_exhibitors.csv"
    save_to_csv(exhibitors, str(output_file))

    # Preview first 5
    if exhibitors:
        print("\nFirst 5 exhibitors:")
        for i, ex in enumerate(exhibitors[:5], 1):
            print(f"  {i}. {ex['exhibitor_name']} | {ex['country']} | {ex['sector'][:50]}...")


if __name__ == "__main__":
    main()
