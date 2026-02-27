#!/usr/bin/env python3
"""
Company Contact Enrichment via Google Search
Uses Playwright Chromium in non-headless mode with minimal CAPTCHA risk.
Pauses for manual CAPTCHA solve when detected.
"""

import argparse
import csv
import json
import logging
import random
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from playwright.sync_api import Page, Route, sync_playwright, TimeoutError as PlaywrightTimeout

# ============ Logging ============

LOG_FILE = Path(__file__).parent / "enrichment.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# Track CAPTCHA encounters for reporting
CAPTCHA_ENCOUNTERS: list[dict[str, str]] = []

# ============ Configuration ============

EXCLUDED_DOMAINS = {
    "linkedin.com", "facebook.com", "fb.com", "twitter.com", "x.com",
    "crunchbase.com", "wikipedia.org", "wikimedia.org", "youtube.com",
    "instagram.com", "pinterest.com", "reddit.com", "medium.com",
    "bloomberg.com", "reuters.com", "bbc.com", "cnn.com", "nytimes.com",
    "theguardian.com", "forbes.com", "techcrunch.com", "businesswire.com",
    "prnewswire.com", "zoominfo.com", "duckduckgo.com", "google.com",
}

EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", re.I)
PHONE_PATTERN = re.compile(
    r"(?:\+?\d{1,4}[-.\s]?)?(?:\(?\d{2,4}\)?[-.\s]?)?\d{3,4}[-.\s]?\d{3,4}(?:[-.\s]?\d{2,4})?",
    re.MULTILINE,
)
PHONE_STRICT = re.compile(r"\+\d{1,4}[-.\s]?\d{2,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}(?:[-.\s]?\d{2,4})?")
LINKEDIN_P = re.compile(r"https?://(?:www\.)?linkedin\.com/[^\s\"'<>]+", re.I)
TWITTER_P = re.compile(r"https?://(?:www\.)?(?:twitter\.com|x\.com)/[^\s\"'<>]+", re.I)
FACEBOOK_P = re.compile(r"https?://(?:www\.)?(?:facebook|fb)\.com/[^\s\"'<>]+", re.I)

CONTACT_PATHS = ("/contact", "/contact-us", "/about", "/about-us", "/get-in-touch", "/reach-us", "/support")
CONTACT_LINK_HINTS = ["contact", "contact us", "about", "about us", "reach us", "get in touch", "support"]

DELAY_MIN, DELAY_MAX = 2.0, 5.0
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


# ============ Data Models ============

@dataclass
class CompanyInput:
    company_name: str
    country: str
    sector: str = ""


@dataclass
class CompanyResult:
    company_name: str
    country: str
    sector: str
    website: str = ""
    website_confidence: float = 0.0
    emails: list[str] = field(default_factory=list)
    phones: list[str] = field(default_factory=list)
    address: str = ""
    social_links: list[str] = field(default_factory=list)
    source: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "company_name": self.company_name,
            "country": self.country,
            "sector": self.sector,
            "website": self.website,
            "website_confidence": round(self.website_confidence, 2),
            "emails": self.emails,
            "phones": self.phones,
            "address": self.address,
            "social_links": self.social_links,
            "source": self.source,
        }


# ============ Extraction Helpers ============

def strip_html(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"<[^>]+>", " ", text).replace("&nbsp;", " ").replace("&amp;", "&")


def extract_emails(text: str) -> list[str]:
    if not text:
        return []
    text = strip_html(text)
    found = set(EMAIL_PATTERN.findall(text))
    skip = ("example.com", "test.com", "duckduckgo.com", "google.com", "wixpress.com")
    return sorted(e.lower() for e in found if not any(s in e.lower() for s in skip) and len(e) > 5)


def extract_phones(text: str) -> list[str]:
    if not text:
        return []
    text = strip_html(text)
    valid = []
    for m in PHONE_STRICT.finditer(text):
        p = m.group(0).strip()
        digits = re.sub(r"\D", "", p)
        if 10 <= len(digits) <= 15:
            valid.append(p)
    if not valid:
        for m in PHONE_PATTERN.finditer(text):
            p = m.group(0).strip()
            digits = re.sub(r"\D", "", p)
            if 10 <= len(digits) <= 15 and "2147483647" not in digits:
                valid.append(p)
    seen = set()
    ordered = []
    for p in valid:
        d = re.sub(r"\D", "", p)
        if d not in seen:
            seen.add(d)
            ordered.append(p)
    ordered.sort(key=lambda x: (0 if x.startswith("+") else 1, x))
    return ordered[:10]


def extract_social_links(html: str) -> list[str]:
    links = set()
    for p in (LINKEDIN_P, TWITTER_P, FACEBOOK_P):
        links.update(p.findall(html))
    return sorted(links)


# ============ Confidence Scoring ============

def score_website_confidence(url: str, company_name: str, country: str) -> float:
    """Return confidence score 0–1 for website URL."""
    try:
        parsed = urlparse(url)
        domain = (parsed.netloc or "").lower().replace("www.", "")
        path = (parsed.path or "").lower()
    except Exception:
        return 0.0

    for ex in EXCLUDED_DOMAINS:
        if ex in domain:
            return 0.0

    score = 0.0
    company_slug = re.sub(r"[^a-z0-9]", "", company_name.lower())[:15]
    if company_slug and company_slug in domain:
        score += 0.5
    if len(domain.split(".")) <= 2:
        score += 0.2
    if any(x in path for x in ["/news/", "/blog/", "/article/"]):
        score -= 0.3
    return min(1.0, max(0.0, 0.3 + score))


def pick_best_website(urls: list[str], company_name: str, country: str) -> tuple[str, float]:
    """Select best URL with confidence score."""
    scored = [(u, score_website_confidence(u, company_name, country)) for u in urls]
    scored = [(u, s) for u, s in scored if s > 0]
    if not scored:
        return (urls[0], 0.0) if urls else ("", 0.0)
    best = max(scored, key=lambda x: x[1])
    return best[0], best[1]


# ============ Resource Blocking ============

def block_resources(route: Route) -> None:
    rt = route.request.resource_type
    if rt in ("image", "media", "font"):
        route.abort()
    else:
        route.continue_()


# ============ Delays ============

def random_delay() -> None:
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


# ============ Google Consent ============

def handle_consent(page: Page) -> None:
    try:
        for text in ["Accept all", "I agree", "Accept", "Agree", "Accept All"]:
            btn = page.get_by_role("button", name=re.compile(re.escape(text), re.I))
            if btn.count() > 0:
                btn.first.click(timeout=4000)
                random_delay()
                return
    except Exception:
        pass


# ============ CAPTCHA Detection & Pause ============

def detect_recaptcha(page: Page) -> bool:
    """Detect if reCAPTCHA is present. Returns True if user must solve manually."""
    try:
        html = page.content()
        if "recaptcha" not in html.lower() and "unusual traffic" not in html.lower():
            return False
        if page.locator(".g-recaptcha, #captcha-form, iframe[src*='recaptcha']").count() > 0:
            return True
        if len(html) < 20000:
            return True
    except Exception:
        pass
    return False


def wait_for_captcha_solve(page: Page, company_name: str) -> None:
    """Pause and wait for user to manually solve CAPTCHA."""
    CAPTCHA_ENCOUNTERS.append({"company": company_name, "url": page.url})
    logger.warning("[CAPTCHA] Detected reCAPTCHA for company '%s'. Solve it manually in the browser.", company_name)
    logger.warning("[CAPTCHA] Press Enter here when done to continue...")
    input()
    random_delay()


# ============ Knowledge Panel Extraction ============

def extract_knowledge_panel(page: Page) -> dict[str, Any]:
    """Extract visible contact info from Google knowledge panel if present."""
    data: dict[str, Any] = {"website": "", "phone": "", "address": ""}
    try:
        # Knowledge panel typically in right sidebar or specific divs
        for sel in ["[data-attrid='kc:/location/location:address']", "[data-attrid='og:website']",
                    ".knowledge-panel", "[role='complementary']", ".kp-wholepage"]:
            try:
                el = page.locator(sel).first
                if el.count() > 0:
                    txt = el.inner_text(timeout=2000)
                    if txt:
                        data["address"] = data["address"] or extract_address_heuristic(txt)
                        data["phone"] = data["phone"] or (extract_phones(txt)[:1] or [""])[0]
            except Exception:
                continue
        # Links in knowledge panel
        for a in page.locator("a[href^='http']").all():
            try:
                href = a.get_attribute("href")
                if href and "google" not in href:
                    d = (urlparse(href).netloc or "").lower()
                    if not any(ex in d for ex in EXCLUDED_DOMAINS) and "maps" not in href:
                        if not data["website"] or len(d) < len(data["website"]):
                            data["website"] = href
            except Exception:
                continue
    except Exception as e:
        logger.debug("Knowledge panel extraction: %s", e)
    return data


def extract_address_heuristic(text: str) -> str:
    """Heuristic: look for address-like patterns."""
    lines = [l.strip() for l in text.split("\n") if len(l.strip()) > 10 and len(l.strip()) < 200]
    for line in lines:
        if re.search(r"\d{4,6}", line) and (re.search(r"\b(street|st|road|rd|avenue|ave|floor|fl)\b", line, re.I) or "," in line):
            return " ".join(line.split())[:250]
    for line in lines:
        if "headquartered" in line.lower() or "located" in line.lower() or "address" in line.lower():
            return " ".join(line.split())[:250]
    return ""


# ============ Organic Results Extraction ============

def extract_organic_urls(page: Page, company_name: str, country: str) -> tuple[list[str], dict[str, Any]]:
    """Extract URLs from organic results and knowledge panel."""
    urls: list[str] = []
    kp_data: dict[str, Any] = {}

    try:
        kp_data = extract_knowledge_panel(page)
        if kp_data.get("website"):
            urls.append(kp_data["website"])

        content = page.content()
        for a in page.locator('a[href^="http"]').all():
            try:
                href = a.get_attribute("href")
                if not href or "google.com" in href or "accounts.google" in href:
                    continue
                domain = (urlparse(href).netloc or "").lower()
                if any(ex in domain for ex in EXCLUDED_DOMAINS):
                    continue
                urls.append(href)
            except Exception:
                continue

        # Also extract from page HTML (AI Overview, snippets)
        urls.extend(re.findall(r'https?://(?:www\.)?[a-zA-Z0-9][-a-zA-Z0-9]*\.(?:com|org|net|io|co|in)/[^\s"\'<>]*', content))
    except Exception as e:
        logger.warning("Organic extraction error: %s", e)

    urls = list(dict.fromkeys(u for u in urls if not any(ex in u.lower() for ex in EXCLUDED_DOMAINS)))
    return urls[:20], kp_data


# ============ Website Scraping ============

def find_contact_page(page: Page, base_url: str) -> str | None:
    """Find contact-related page URL from current page links (no navigation)."""
    try:
        # Footer links first
        for a in page.locator("footer a[href], [role='contentinfo'] a[href]").all():
            try:
                href = a.get_attribute("href")
                text = (a.inner_text() or "").lower()
                if href and any(h in text for h in CONTACT_LINK_HINTS):
                    full = urljoin(base_url, href)
                    if full.startswith("http") and "mailto:" not in full:
                        return full
            except Exception:
                continue

        # Any link with contact/about text
        for a in page.locator("a[href]").all():
            try:
                href = a.get_attribute("href")
                text = (a.inner_text() or "").lower()
                if href and any(h in text for h in CONTACT_LINK_HINTS):
                    full = urljoin(base_url, href)
                    if full.startswith("http") and "mailto:" not in full and "tel:" not in full:
                        return full
            except Exception:
                continue
    except Exception as e:
        logger.debug("Contact page find: %s", e)
    return None


def scrape_website(page: Page, url: str) -> dict[str, Any]:
    """Scrape contact info from company website."""
    data: dict[str, Any] = {"emails": [], "phones": [], "address": "", "social_links": []}
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_load_state("domcontentloaded")
        random_delay()

        html = page.content()
        data["emails"] = extract_emails(html)
        data["phones"] = extract_phones(html)
        data["social_links"] = extract_social_links(html)

        # Address from structured elements
        for sel in ['[itemprop="address"]', "address", "footer", '[class*="address"]']:
            try:
                el = page.locator(sel).first
                if el.count() > 0:
                    txt = el.inner_text()
                    if txt and 15 < len(txt) < 400:
                        addr = extract_address_heuristic(txt)
                        if addr:
                            data["address"] = addr
                            break
            except Exception:
                continue

        # mailto
        for a in page.locator('a[href^="mailto:"]').all():
            try:
                h = a.get_attribute("href")
                if h:
                    e = h.replace("mailto:", "").split("?")[0].strip()
                    if "@" in e and e not in data["emails"]:
                        data["emails"].append(e.lower())
            except Exception:
                continue

        # Contact page: from links or common paths
        contact_url = find_contact_page(page, url)
        if not contact_url:
            base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            for path in CONTACT_PATHS:
                try:
                    test = urljoin(base, path)
                    r = page.goto(test, wait_until="domcontentloaded", timeout=5000)
                    if r and r.status == 200:
                        contact_url = test
                        break
                except Exception:
                    page.goto(url, wait_until="domcontentloaded", timeout=5000)
                    continue
        if contact_url and contact_url != url:
            try:
                page.goto(contact_url, wait_until="domcontentloaded", timeout=10000)
                random_delay()
                html2 = page.content()
                data["emails"] = list(dict.fromkeys(data["emails"] + extract_emails(html2)))
                data["phones"] = list(dict.fromkeys(data["phones"] + extract_phones(html2)))
                data["social_links"] = list(dict.fromkeys(data["social_links"] + extract_social_links(html2)))
                if not data["address"]:
                    for sel in ["address", "footer", '[itemprop="address"]']:
                        try:
                            el = page.locator(sel).first
                            if el.count() > 0:
                                txt = el.inner_text()
                                if txt and 15 < len(txt) < 400:
                                    data["address"] = extract_address_heuristic(txt)
                                    break
                        except Exception:
                            continue
            except Exception:
                pass
    except PlaywrightTimeout:
        logger.warning("Timeout scraping %s", url)
    except Exception as e:
        logger.warning("Website scrape error %s: %s", url, e)

    return data


# ============ Main Enrichment Flow ============

def enrich_company(pw: Any, company: CompanyInput) -> CompanyResult:
    """Enrich a single company. New browser per company to avoid repeated session."""
    result = CompanyResult(
        company_name=company.company_name,
        country=company.country,
        sector=company.sector,
    )

    browser = pw.chromium.launch(headless=False)
    context = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent=USER_AGENT,
        locale="en-IN",
        timezone_id="Asia/Kolkata",
    )
    page = context.new_page()
    page.route("**/*", block_resources)

    try:
        query = f'"{company.company_name}" {company.country} official website contact'
        logger.info("Processing: %s", company.company_name)

        # Google
        page.goto("https://www.google.com", wait_until="domcontentloaded", timeout=15000)
        page.wait_for_load_state("domcontentloaded")
        random_delay()
        handle_consent(page)
        random_delay()

        # Search
        search = page.locator('textarea[name="q"], input[name="q"]').first
        search.fill(query)
        random_delay()
        search.press("Enter")
        page.wait_for_load_state("networkidle", timeout=15000)
        random_delay()

        # CAPTCHA check
        if detect_recaptcha(page):
            wait_for_captcha_solve(page, company.company_name)
            page.wait_for_load_state("domcontentloaded")
            random_delay()

        # Extract
        urls, kp = extract_organic_urls(page, company.company_name, company.country)
        result.emails = extract_emails(page.content())
        result.phones = extract_phones(page.content())
        result.social_links = extract_social_links(page.content())
        result.address = kp.get("address") or extract_address_heuristic(page.content())

        if kp.get("phone"):
            result.phones = list(dict.fromkeys([kp["phone"]] + result.phones))
        if kp.get("website"):
            urls = list(dict.fromkeys([kp["website"]] + urls))

        # Pick website
        if urls:
            best_url, confidence = pick_best_website(urls, company.company_name, company.country)
            result.website = best_url
            result.website_confidence = confidence
            result.source = ["google"]

        # Visit website
        if result.website:
            try:
                site_data = scrape_website(page, result.website)
                result.emails = list(dict.fromkeys(result.emails + site_data["emails"]))
                result.phones = list(dict.fromkeys(result.phones + site_data["phones"]))
                result.social_links = list(dict.fromkeys(result.social_links + site_data["social_links"]))
                if site_data["address"]:
                    result.address = result.address or site_data["address"]
                if "website" not in result.source:
                    result.source.append("website")
            except Exception as e:
                logger.warning("Website visit failed for %s: %s", company.company_name, e)

    except PlaywrightTimeout as e:
        logger.error("Timeout for %s: %s", company.company_name, e)
    except Exception as e:
        logger.error("Error for %s: %s", company.company_name, e)
    finally:
        context.close()
        browser.close()

    return result


# ============ I/O ============

def load_companies(path: str) -> list[CompanyInput]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    companies = []
    ext = p.suffix.lower()
    if ext == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        items = data if isinstance(data, list) else data.get("companies", [data])
        for i in items:
            n = i.get("company_name") or i.get("exhibitor_name") or i.get("name", "")
            if n:
                companies.append(CompanyInput(n.strip(), str(i.get("country", "")).strip(), str(i.get("sector", "")).strip()))
    elif ext in (".csv", ".txt"):
        with open(p, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                n = row.get("company_name") or row.get("exhibitor_name") or row.get("name", "")
                if n:
                    companies.append(CompanyInput(n.strip(), str(row.get("country", "")).strip(), str(row.get("sector", "")).strip()))
    else:
        raise ValueError("Use .json or .csv")
    return companies


def save_results(results: list[CompanyResult], base: str) -> None:
    basepath = Path(base)
    data = [r.to_dict() for r in results]

    with open(basepath.with_suffix(".json"), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    if data:
        with open(basepath.with_suffix(".csv"), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(data[0].keys()))
            w.writeheader()
            for d in data:
                row = {k: (", ".join(v) if isinstance(v, list) else v) for k, v in d.items()}
                w.writerow(row)

    logger.info("Saved %s.json and %s.csv", basepath.with_suffix(".json"), basepath.with_suffix(".csv"))

    if CAPTCHA_ENCOUNTERS:
        cap_file = basepath.parent / "captcha_encounters.json"
        with open(cap_file, "w", encoding="utf-8") as f:
            json.dump(CAPTCHA_ENCOUNTERS, f, indent=2)
        logger.info("CAPTCHA encounters logged to %s", cap_file)


# ============ Entry ============

def main() -> None:
    parser = argparse.ArgumentParser(description="Company contact enrichment via Google (non-headless)")
    parser.add_argument("input", help="Input JSON or CSV")
    parser.add_argument("-o", "--output", default="enrichment_results", help="Output base path")
    parser.add_argument("--max", type=int, default=0, help="Max companies (0=all)")
    args = parser.parse_args()

    companies = load_companies(args.input)
    if args.max:
        companies = companies[: args.max]
    logger.info("Loaded %d companies", len(companies))

    results = []
    with sync_playwright() as pw:
        for i, c in enumerate(companies, 1):
            logger.info("[%d/%d] %s", i, len(companies), c.company_name)
            results.append(enrich_company(pw, c))
            if i < len(companies):
                random_delay()

    save_results(results, args.output)
    if CAPTCHA_ENCOUNTERS:
        logger.info("CAPTCHA was encountered %d time(s). See captcha_encounters.json", len(CAPTCHA_ENCOUNTERS))


if __name__ == "__main__":
    main()
