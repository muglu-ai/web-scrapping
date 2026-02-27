#!/usr/bin/env python3
"""
Company Contact Scraper
Uses Google Search + official website scraping to extract contact details (emails, phones, addresses, social links).
Input: JSON or CSV with company_name, country, sector
Output: JSON and CSV with structured contact data
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
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

import requests

from playwright.sync_api import Page, Route, sync_playwright, TimeoutError as PlaywrightTimeout

# ============ Configuration ============

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Domains to exclude when picking the official website
EXCLUDED_DOMAINS = {
    "linkedin.com",
    "facebook.com",
    "fb.com",
    "twitter.com",
    "x.com",
    "crunchbase.com",
    "wikipedia.org",
    "wikipedia.com",
    "wikimedia.org",
    "youtube.com",
    "instagram.com",
    "pinterest.com",
    "tiktok.com",
    "reddit.com",
    "medium.com",
    "slideshare.net",
    "bloomberg.com",
    "reuters.com",
    "bbc.com",
    "cnn.com",
    "nytimes.com",
    "theguardian.com",
    "forbes.com",
    "techcrunch.com",
    "businesswire.com",
    "prnewswire.com",
    "press-release",
    "news.",
    "blog.",
    "duckduckgo.com",
    "w3.org",
    "w3.org.",
}

# Third-party directories to exclude when picking official website
for _d in ("zoominfo.com", "dial4trade.com", "zaubacorp.com", "cleartax.in",
           "salezshark.com", "craft.co", "insiderbiz.in", "indiabiz.info",
           "gust.com", "glassdoor.com", "emis.com", "indiamart.com", "f6s.com",
           "bulwarktech.com", "bdsoft.in", "datanyze.com"):
    EXCLUDED_DOMAINS.add(_d)

# Regex patterns for extraction
EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE
)
PHONE_PATTERN = re.compile(
    r"(?:\+?\d{1,4}[-.\s]?)?(?:\(?\d{2,4}\)?[-.\s]?)?\d{3,4}[-.\s]?\d{3,4}(?:[-.\s]?\d{2,4})?",
    re.MULTILINE
)
# Social media URL patterns
LINKEDIN_PATTERN = re.compile(r"https?://(?:www\.)?linkedin\.com/[^\s\"'<>]+", re.I)
TWITTER_PATTERN = re.compile(r"https?://(?:www\.)?(?:twitter\.com|x\.com)/[^\s\"'<>]+", re.I)
FACEBOOK_PATTERN = re.compile(r"https?://(?:www\.)?(?:facebook|fb)\.com/[^\s\"'<>]+", re.I)

# Contact-related link text hints
CONTACT_LINK_HINTS = ["contact", "contact us", "about", "about us", "reach us", "get in touch", "support"]

# Realistic delay range (seconds)
MIN_DELAY = 1.5
MAX_DELAY = 3.5


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
            "emails": self.emails,
            "phones": self.phones,
            "address": self.address,
            "social_links": self.social_links,
            "source": self.source,
        }


# ============ URL Scoring ============

def score_website_url(url: str, company_name: str, country: str) -> float:
    """
    Score a URL to pick the best candidate for the official company website.
    Higher score = more likely to be the official site.
    """
    try:
        parsed = urlparse(url)
        domain = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
    except Exception:
        return -1000.0

    score = 0.0

    # Exclude known aggregator/social domains
    for excluded in EXCLUDED_DOMAINS:
        if excluded in domain or domain.endswith("." + excluded):
            return -1000.0

    # Prefer shorter, cleaner domains
    domain_parts = domain.replace("www.", "").split(".")
    base_domain = domain_parts[0] if domain_parts else ""
    if len(domain_parts) <= 2:
        score += 2.0  # company.com vs sub.company.co.uk

    # Boost if company name (simplified) appears in domain
    company_slug = re.sub(r"[^a-z0-9]", "", company_name.lower())[:15]
    if company_slug and company_slug in domain:
        score += 5.0
    # Country TLD or in domain can help
    country_lower = country.lower().replace(" ", "")[:10]
    if country_lower and (country_lower in domain or any(c in domain for c in ["uk", "de", "fr", "in", "ae"])):
        score += 0.5

    # Penalize long paths (often article/news pages)
    if len(path) > 30:
        score -= 1.0
    if any(x in path for x in ["/news/", "/blog/", "/article/", "/tag/", "/author/"]):
        score -= 2.0

    # Penalize known low-value TLDs
    if domain.endswith(".pdf") or domain.endswith(".doc"):
        score -= 5.0

    return score


def pick_best_website(urls: list[str], company_name: str, country: str) -> str:
    """From a list of URLs, return the one with the highest score."""
    scored = [(url, score_website_url(url, company_name, country)) for url in urls]
    scored = [(u, s) for u, s in scored if s > 0]
    if not scored:
        return urls[0] if urls else ""
    return max(scored, key=lambda x: x[1])[0]


# ============ Extraction Helpers ============

def strip_html(text: str) -> str:
    """Remove HTML tags from text."""
    if not text:
        return ""
    return re.sub(r"<[^>]+>", " ", text).replace("&nbsp;", " ").replace("&amp;", "&")


def extract_emails(text: str) -> list[str]:
    """Extract and normalize email addresses from text."""
    if not text:
        return []
    text = strip_html(text)
    found = set(EMAIL_PATTERN.findall(text))
    # Basic validation - filter obvious non-emails
    skip_domains = ("example.com", "test.com", "domain.com", "duckduckgo.com", "wixpress.com")
    skip_suffixes = (".png", ".jpg", ".gif", "xxx", "sentry.io", "google.com")
    valid = []
    for e in found:
        e = e.strip().lower()
        if len(e) > 5 and "@" in e and "." in e.split("@")[-1]:
            if not any(x in e for x in skip_domains):
                if not any(e.endswith(x) or x in e for x in skip_suffixes):
                    valid.append(e)
    return sorted(set(valid))


# Stricter patterns for real phone numbers (reduces false positives from prices, IDs)
PHONE_INTERNATIONAL = re.compile(r"\+\d{1,4}[-.\s]?\d{2,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}(?:[-.\s]?\d{2,4})?")
PHONE_INDIAN = re.compile(r"(?:\+91|91|0)?[-.\s]?\d{4,5}[-.\s]?\d{5}")


def extract_phones(text: str) -> list[str]:
    """Extract and normalize phone numbers from text. Prefers international/Indian formats."""
    if not text:
        return []
    text = strip_html(text)
    valid = []
    for pattern in (PHONE_INTERNATIONAL, PHONE_INDIAN):
        for m in pattern.finditer(text):
            p = m.group(0).strip()
            digits = re.sub(r"\D", "", p)
            if 10 <= len(digits) <= 15:
                valid.append(p)
    if not valid:
        for m in PHONE_PATTERN.finditer(text):
            p = m.group(0).strip()
            digits = re.sub(r"\D", "", p)
            if 10 <= len(digits) <= 12 and not any(c in p for c in [".", "e", "E"]):
                valid.append(p)
    # Prefer international format (+XX) and deduplicate; filter junk
    junk_digits = {"2147483647", "1234567890", "12345678901", "9999999999"}
    seen = set()
    ordered = []
    for p in valid:
        digits = re.sub(r"\D", "", p)
        if digits in seen or digits in junk_digits:
            continue
        seen.add(digits)
        ordered.append(p)
    # Put +XX numbers first
    ordered.sort(key=lambda x: (0 if x.strip().startswith("+") else 1, x))
    return ordered[:10]  # Limit to 10 most likely


def extract_social_links(html: str) -> list[str]:
    """Extract LinkedIn, Twitter, Facebook URLs from page."""
    links = set()
    for pattern in (LINKEDIN_PATTERN, TWITTER_PATTERN, FACEBOOK_PATTERN):
        links.update(pattern.findall(html))
    return sorted(links)


# Domain pattern for bare domains in text (e.g. "42gears.com" in AI Overview)
DOMAIN_PATTERN = re.compile(
    r"(?:https?://)?(?:www\.)?([a-zA-Z0-9][-a-zA-Z0-9]*\.(?:com|org|net|io|co|in|de|uk|fr|ae)[^\s\"'<>]*)",
    re.IGNORECASE
)
# Address/location hints in text
ADDRESS_HINT_PATTERN = re.compile(
    r"(?:headquartered in|based in|located in|office in|HQ in)\s+([^.]+?)(?:\.|$)",
    re.IGNORECASE
)


def extract_domains_from_text(text: str) -> list[str]:
    """Extract website domains from text (e.g. AI Overview '42gears.com')."""
    if not text:
        return []
    text = strip_html(text)
    found = []
    for m in DOMAIN_PATTERN.finditer(text):
        domain = m.group(1).rstrip(".,;:)")
        if "/" in domain:
            domain = domain.split("/")[0]
        domain = domain.lower()
        if not any(ex in domain for ex in EXCLUDED_DOMAINS):
            full = f"https://{domain}" if not domain.startswith("http") else domain
            found.append(full)
    return list(dict.fromkeys(found))


def extract_address_from_text(text: str) -> str:
    """Extract address/location hints like 'headquartered in Bengaluru'."""
    if not text:
        return ""
    text = strip_html(text)
    for m in ADDRESS_HINT_PATTERN.finditer(text):
        addr = m.group(1).strip()
        if 3 < len(addr) < 150:
            return addr
    return ""


def get_ai_overview_text(page: Page) -> str:
    """
    Extract text from Google's AI Overview section if present.
    Uses flexible text-based locators; traverses DOM to find the overview block.
    """
    for marker in ["AI Overview", "Key Contact Information"]:
        try:
            loc = page.get_by_text(marker, exact=False).first
            # Use evaluate to traverse up and find a parent block with substantial content
            txt = loc.evaluate(
                """
                el => {
                    let p = el;
                    for (let i = 0; i < 8 && p; i++) {
                        const text = p.innerText || '';
                        if (text.length > 150 && text.length < 8000) return text;
                        p = p.parentElement;
                    }
                    return '';
                }
                """
            )
            if txt and len(txt) > 150:
                return txt
        except Exception:
            continue
    return ""


def normalize_result(result: CompanyResult) -> CompanyResult:
    """Deduplicate and normalize extracted data."""
    result.emails = list(dict.fromkeys(result.emails))
    result.phones = list(dict.fromkeys(result.phones))
    result.social_links = list(dict.fromkeys(result.social_links))
    if result.address:
        result.address = " ".join(result.address.split())
    return result


# ============ CAPTCHA & Fallback Detection ============

def is_google_captcha_page(page: Page) -> bool:
    """Detect if Google returned CAPTCHA instead of search results."""
    try:
        html = page.content()
        if "recaptcha" in html.lower() or "unusual traffic" in html.lower():
            if len(html) < 20000:  # Real SERP is usually 50KB+
                return True
            if page.locator("#captcha-form, .g-recaptcha").count() > 0:
                return True
            if page.locator("div.g").count() == 0:  # No organic results
                return True
    except Exception:
        pass
    return False


def fetch_duckduckgo_html(query: str) -> str:
    """Fetch DuckDuckGo HTML via requests (avoids browser detection)."""
    url = f"https://html.duckduckgo.com/html/?q={quote(query)}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    return r.text


def extract_from_duckduckgo(page: Page, company_name: str, country: str) -> tuple[list[str], CompanyResult]:
    """
    Search via DuckDuckGo HTML. Uses requests (not Playwright) to avoid bot detection.
    Extracts URLs from uddg redirect links and contact info from snippets.
    """
    result = CompanyResult(
        company_name=company_name,
        country=country,
        sector="",
        source=["duckduckgo"],
    )
    candidate_urls: list[str] = []

    try:
        # Shorter query ranks official site higher; "contact" skews DDG to directories
        query = f'"{company_name}" {country} official website'
        html = fetch_duckduckgo_html(query)
        _random_delay(0.5, 1)

        # Skip if DDG returned error page
        if "Error getting results" in html or len(html) < 3000:
            logger.warning("DuckDuckGo returned error or empty page")
            return candidate_urls, result

        # Extract target URLs from DuckDuckGo redirect links: ...uddg=URL_ENCODED...
        uddg_pattern = re.compile(r'uddg=([^&"\']+)')
        for m in uddg_pattern.finditer(html):
            try:
                decoded = unquote(m.group(1))
                if not decoded.startswith("http"):
                    continue
                if "duckduckgo" in decoded.lower() or "subject=" in decoded or "feedback" in decoded:
                    continue
                parsed = urlparse(decoded)
                domain = (parsed.netloc or "").lower()
                if any(ex in domain for ex in EXCLUDED_DOMAINS):
                    continue
                candidate_urls.append(decoded)
            except Exception:
                continue

        # Extract contact info from page text (snippets contain emails, phones, addresses)
        result.emails = extract_emails(html)
        result.phones = extract_phones(html)
        result.social_links = extract_social_links(html)
        result.address = extract_address_from_text(html) or ""
        ai_domains = extract_domains_from_text(html)
        if ai_domains:
            candidate_urls = list(dict.fromkeys(ai_domains + candidate_urls))

        if candidate_urls:
            best = pick_best_website(candidate_urls[:20], company_name, country)
            if best and "duckduckgo" not in best.lower():
                result.website = best
    except Exception as e:
        logger.warning("DuckDuckGo extraction error: %s", e)

    return candidate_urls, result


# ============ Google Consent ============

def handle_google_consent(page: Page) -> None:
    """Dismiss Google consent/cookie popup if present."""
    try:
        # Try common consent button texts (vary by locale)
        for text in ["Accept all", "I agree", "Accept", "Agree", "Accept All", "Allow all"]:
            btn = page.get_by_role("button", name=re.compile(re.escape(text), re.I))
            if btn.count() > 0:
                btn.first.click(timeout=3000)
                _random_delay(1, 2)
                return
        # Fallback: try form submit
        form = page.locator("form").filter(has_text=re.compile("consent|accept|agree", re.I)).first
        if form.count() > 0:
            form.get_by_role("button").first.click(timeout=2000)
    except Exception:
        pass  # No consent popup or already dismissed


# ============ Google Search ============

def _random_delay(lo: float = MIN_DELAY, hi: float = MAX_DELAY) -> None:
    time.sleep(random.uniform(lo, hi))


def extract_from_google_results(page: Page, company_name: str, country: str) -> tuple[list[str], CompanyResult]:
    """
    Extract candidate URLs and any visible contact info from Google search results.
    Handles AI Overview section when present for richer contact data.
    Returns (list of candidate URLs, partial CompanyResult with google-sourced data).
    """
    result = CompanyResult(
        company_name=company_name,
        country=country,
        sector="",
        source=["google"],
    )
    candidate_urls: list[str] = []

    try:
        # Get full page content for regex extraction
        content = page.content()
        result.emails = extract_emails(content)
        result.phones = extract_phones(content)
        result.social_links = extract_social_links(content)

        # Try to extract from AI Overview section (richer structured data)
        ai_text = get_ai_overview_text(page)
        ai_domains: list[str] = []
        if ai_text:
            logger.debug("Found AI Overview section")
            ai_emails = extract_emails(ai_text)
            ai_phones = extract_phones(ai_text)
            if ai_emails:
                result.emails = list(dict.fromkeys(ai_emails + result.emails))
            if ai_phones:
                result.phones = list(dict.fromkeys(ai_phones + result.phones))
            ai_domains = extract_domains_from_text(ai_text)
            if not result.address:
                result.address = extract_address_from_text(ai_text)

        # Collect organic result links - use flexible selectors
        links = page.locator('a[href^="http"]').all()
        seen = set()

        for link in links:
            try:
                href = link.get_attribute("href")
                if not href or "google.com" in href or "accounts.google" in href or href in seen:
                    continue
                if not href.startswith("http"):
                    continue
                parsed = urlparse(href)
                domain = (parsed.netloc or "").lower()
                if any(ex in domain for ex in EXCLUDED_DOMAINS):
                    continue
                candidate_urls.append(href)
                seen.add(href)
            except Exception:
                continue

        # Pick best URL: prefer organic links, fallback to AI Overview domains
        if candidate_urls:
            best = pick_best_website(candidate_urls[:15], company_name, country)
            result.website = best
        elif ai_text and ai_domains:
            # No good links from results; use domain from AI Overview
            best = pick_best_website(ai_domains, company_name, country)
            if best:
                result.website = best
    except Exception as e:
        logger.warning("Error extracting from Google results: %s", e)

    return candidate_urls, result


# ============ Website Scraping ============

def find_contact_section_url(page: Page, base_url: str) -> str | None:
    """Find a Contact/About page URL from the homepage."""
    try:
        links = page.locator("a[href]").all()
        for link in links:
            try:
                href = link.get_attribute("href")
                text = (link.inner_text() or "").lower()
                if not href or not text:
                    continue
                full_url = urljoin(base_url, href)
                if any(hint in text for hint in CONTACT_LINK_HINTS):
                    if full_url.startswith("http") and "mailto:" not in full_url and "tel:" not in full_url:
                        return full_url
            except Exception:
                continue
    except Exception:
        pass
    return None


def scrape_website_contacts(page: Page, url: str) -> dict[str, Any]:
    """Scrape contact info from a company website."""
    data: dict[str, Any] = {
        "emails": [],
        "phones": [],
        "address": "",
        "social_links": [],
    }

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
        _random_delay(1.5, 2.5)

        html = page.content()
        data["emails"] = extract_emails(html)
        data["phones"] = extract_phones(html)
        data["social_links"] = extract_social_links(html)

        # Try to find address in structured elements
        addr_selectors = [
            '[itemprop="address"]',
            'address',
            '[class*="address"]',
            '[class*="contact"]',
            'footer',
        ]
        for sel in addr_selectors:
            try:
                el = page.locator(sel).first
                if el.count() > 0:
                    txt = el.inner_text()
                    if txt and len(txt) > 10 and len(txt) < 500:
                        phones_in_txt = extract_phones(txt)
                        if phones_in_txt or any(c.isdigit() for c in txt):
                            data["address"] = " ".join(txt.split())[:300]
                            break
            except Exception:
                continue

        # Try Contact page for more data
        contact_url = find_contact_section_url(page, url)
        if contact_url and contact_url != url:
            try:
                page.goto(contact_url, wait_until="domcontentloaded", timeout=10000)
                _random_delay(1, 2)
                html2 = page.content()
                data["emails"].extend(extract_emails(html2))
                data["phones"].extend(extract_phones(html2))
                data["social_links"].extend(extract_social_links(html2))
                if not data["address"]:
                    for sel in addr_selectors:
                        try:
                            el = page.locator(sel).first
                            if el.count() > 0:
                                txt = el.inner_text()
                                if txt and 10 < len(txt) < 500:
                                    data["address"] = " ".join(txt.split())[:300]
                                    break
                        except Exception:
                            continue
            except Exception:
                pass

        # mailto links
        mailto_links = page.locator('a[href^="mailto:"]').all()
        for a in mailto_links:
            try:
                href = a.get_attribute("href")
                if href and "mailto:" in href:
                    email = href.replace("mailto:", "").split("?")[0].strip()
                    if email and "@" in email:
                        data["emails"].append(email.lower())
            except Exception:
                continue

    except Exception as e:
        logger.warning("Error scraping website %s: %s", url, e)

    return data


# ============ Route Blocking (Performance) ============

def block_resources(route: Route) -> None:
    """Block images, fonts, and media to speed up scraping."""
    resource_type = route.request.resource_type
    if resource_type in ("image", "media", "font"):
        route.abort()
    else:
        route.continue_()


# ============ Main Scraper ============

def process_company(
    playwright_context: Any,
    company: CompanyInput,
    block_ads: bool = True,
    use_duckduckgo: bool = False,
    headless: bool = True,
) -> CompanyResult:
    """Process a single company: Google search + website scrape."""
    result = CompanyResult(
        company_name=company.company_name,
        country=company.country,
        sector=company.sector,
    )
    browser = playwright_context.chromium.launch(headless=headless)
    context = browser.new_context(
        viewport={"width": 1280, "height": 720},
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        locale="en-US",
    )
    page = context.new_page()

    if block_ads:
        page.route("**/*", block_resources)

    try:
        query = f'"{company.company_name}" {company.country} official website contact'
        logger.info("Searching: %s", query)

        if use_duckduckgo:
            candidate_urls, search_result = extract_from_duckduckgo(
                page, company.company_name, company.country
            )
        else:
            page.goto("https://www.google.com", wait_until="domcontentloaded", timeout=15000)
            _random_delay(1, 2)
            handle_google_consent(page)
            _random_delay(0.5, 1)

            # Search - try Google first
            search_box = page.get_by_role("combobox", name=re.compile("search", re.I))
            if search_box.count() == 0:
                search_box = page.locator('textarea[name="q"], input[name="q"]')
            search_box.first.fill(query)
            _random_delay(0.3, 0.7)
            search_box.first.press("Enter")
            page.wait_for_load_state("domcontentloaded")
            _random_delay(2, 3)

            # Detect CAPTCHA and fallback to DuckDuckGo
            if is_google_captcha_page(page):
                logger.warning("Google showed CAPTCHA - falling back to DuckDuckGo")
                candidate_urls, search_result = extract_from_duckduckgo(
                    page, company.company_name, company.country
                )
            else:
                candidate_urls, search_result = extract_from_google_results(
                    page, company.company_name, company.country
                )

        result.website = search_result.website
        result.emails = search_result.emails
        result.phones = search_result.phones
        result.address = search_result.address or ""
        result.social_links = search_result.social_links
        result.source = search_result.source

        # Visit official website if we found one
        if result.website:
            website_data = scrape_website_contacts(page, result.website)
            result.emails = list(dict.fromkeys(result.emails + website_data["emails"]))
            result.phones = list(dict.fromkeys(result.phones + website_data["phones"]))
            result.social_links = list(dict.fromkeys(result.social_links + website_data["social_links"]))
            if website_data["address"]:
                result.address = result.address or website_data["address"]
            if "website" not in result.source:
                result.source.append("website")

    except PlaywrightTimeout as e:
        logger.error("Timeout for %s: %s", company.company_name, e)
    except Exception as e:
        logger.error("Error processing %s: %s", company.company_name, e)
    finally:
        context.close()
        browser.close()

    return normalize_result(result)


# ============ I/O ============

def load_companies(path: str) -> list[CompanyInput]:
    """Load companies from JSON or CSV file."""
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    companies: list[CompanyInput] = []
    ext = path_obj.suffix.lower()

    if ext == ".json":
        with open(path_obj, encoding="utf-8") as f:
            data = json.load(f)
        items = data if isinstance(data, list) else data.get("companies", [data])
        for item in items:
            name = item.get("company_name") or item.get("exhibitor_name") or item.get("name", "")
            country = item.get("country", "")
            sector = item.get("sector", "")
            if name:
                companies.append(CompanyInput(company_name=name.strip(), country=str(country).strip(), sector=str(sector).strip()))

    elif ext in (".csv", ".txt"):
        with open(path_obj, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get("company_name") or row.get("exhibitor_name") or row.get("name", "")
                country = row.get("country", "")
                sector = row.get("sector", "")
                if name:
                    companies.append(CompanyInput(company_name=name.strip(), country=str(country).strip(), sector=str(sector).strip()))

    else:
        raise ValueError(f"Unsupported format: {ext}. Use .json or .csv")

    return companies


def save_results(results: list[CompanyResult], output_base: str) -> None:
    """Save results to JSON and CSV."""
    base = Path(output_base)
    json_path = base.with_suffix(".json")
    csv_path = base.with_suffix(".csv")

    # JSON
    data = [r.to_dict() for r in results]
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info("Saved JSON: %s", json_path)

    # CSV
    if not data:
        return
    fieldnames = list(data[0].keys())
    # Flatten lists for CSV
    rows = []
    for d in data:
        row = {k: (", ".join(v) if isinstance(v, list) else v) for k, v in d.items()}
        rows.append(row)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Saved CSV: %s", csv_path)


# ============ Entry Point ============

def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape company contact details via Google + website")
    parser.add_argument("input", help="Input JSON or CSV file with company_name, country, sector")
    parser.add_argument("-o", "--output", default="company_contacts", help="Output base path (without extension)")
    parser.add_argument("--no-block", action="store_true", help="Do not block images/ads (slower)")
    parser.add_argument("--max", type=int, default=0, help="Max companies to process (0=all)")
    parser.add_argument("--duckduckgo", action="store_true", help="Use DuckDuckGo instead of Google (avoids CAPTCHA)")
    parser.add_argument("--headed", action="store_true", help="Run browser in headed mode (visible)")
    args = parser.parse_args()

    companies = load_companies(args.input)
    if args.max:
        companies = companies[: args.max]
    logger.info("Loaded %d companies", len(companies))

    results: list[CompanyResult] = []
    with sync_playwright() as p:
        for i, company in enumerate(companies, 1):
            logger.info("[%d/%d] Processing: %s", i, len(companies), company.company_name)
            result = process_company(
                p, company,
                block_ads=not args.no_block,
                use_duckduckgo=args.duckduckgo,
                headless=not args.headed,
            )
            results.append(result)
            # Brief pause between companies
            if i < len(companies):
                time.sleep(random.uniform(2, 4))

    save_results(results, args.output)
    logger.info("Done. Processed %d companies.", len(results))


if __name__ == "__main__":
    main()
