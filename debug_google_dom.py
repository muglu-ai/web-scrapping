#!/usr/bin/env python3
"""
Debug script: Navigate to Google search, save HTML, and log what we can find.
Run: python debug_google_dom.py
"""

import json
import re
from pathlib import Path

from playwright.sync_api import sync_playwright

OUTPUT_DIR = Path(__file__).parent / "debug_output"
OUTPUT_DIR.mkdir(exist_ok=True)


def main():
    query = '"42Gears" India official website contact'
    print(f"Searching: {query}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = context.new_page()

        try:
            page.goto("https://www.google.com", wait_until="networkidle", timeout=15000)

            # Consent
            for btn_text in ["Accept all", "I agree", "Accept"]:
                try:
                    btn = page.get_by_role("button", name=re.compile(btn_text, re.I))
                    if btn.count() > 0:
                        btn.first.click(timeout=3000)
                        page.wait_for_timeout(2000)
                        break
                except Exception:
                    pass

            # Search
            search = page.locator('textarea[name="q"], input[name="q"]').first
            search.fill(query)
            search.press("Enter")
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(4000)

            # Save full HTML
            html = page.content()
            (OUTPUT_DIR / "google_results.html").write_text(html, encoding="utf-8")
            print(f"Saved HTML ({len(html)} chars) to {OUTPUT_DIR / 'google_results.html'}")

            # Try various selectors and log findings
            findings = {}

            # Links with href
            links = page.locator('a[href^="http"]').all()
            hrefs = []
            for lnk in links[:30]:
                try:
                    h = lnk.get_attribute("href")
                    if h and "google.com" not in h and "accounts.google" not in h:
                        hrefs.append(h)
                except Exception:
                    pass
            findings["http_links"] = hrefs[:20]

            # Text containing email
            if "sales@42gears.com" in html or "@42gears" in html:
                findings["emails_in_html"] = list(set(re.findall(r"[a-zA-Z0-9._%+-]+@42gears\.com", html)))
            else:
                findings["emails_in_html"] = "NOT FOUND - checking regex on full content"
                emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html)
                findings["any_emails_in_html"] = list(set(emails))[:10]

            # AI Overview
            try:
                ai_loc = page.get_by_text("AI Overview", exact=False)
                ai_count = ai_loc.count()
                findings["ai_overview_exists"] = ai_count > 0
                if ai_count > 0:
                    try:
                        txt = ai_loc.first.evaluate("el => el.closest('div')?.innerText?.slice(0,500) || ''")
                        findings["ai_overview_preview"] = (txt[:300] if txt else "empty")
                    except Exception as e:
                        findings["ai_overview_error"] = str(e)
            except Exception as e:
                findings["ai_overview_error"] = str(e)

            # Search for common Google result containers
            for sel in ["div.g", "div[data-hveid]", "div.yuRUbf", "div.VwiC3b", "cite"]:
                try:
                    el = page.locator(sel)
                    c = el.count()
                    findings[f"selector_{sel.replace(' ', '_')}"] = c
                except Exception as e:
                    findings[f"selector_{sel}"] = str(e)

            # Visible text sample
            body_text = page.locator("body").inner_text()
            findings["body_text_length"] = len(body_text)
            findings["body_contains_42gears"] = "42gears" in body_text.lower()
            findings["body_contains_sales"] = "sales@" in body_text.lower()

            # Write findings
            (OUTPUT_DIR / "findings.json").write_text(json.dumps(findings, indent=2), encoding="utf-8")
            print(f"Findings: {json.dumps(findings, indent=2)}")

        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    main()
