# Web Scraping Tools

## 1. GITEX Global 2025 Exhibitor Scraper

Fetches exhibitor details from [exhibitors.gitex.com/gitex-global-2025/Exhibitor](https://exhibitors.gitex.com/gitex-global-2025/Exhibitor).

### Output
- **exhibitor_name** – Company/exhibitor name
- **country** – Country of origin
- **sector** – Product sectors/categories (semicolon-separated)

### Usage
```bash
# Fetch all exhibitors (default)
python gitex_exhibitor_scraper.py

# Limit for testing (e.g., first 200)
python gitex_exhibitor_scraper.py --max 200
```
Output is saved to `gitex_exhibitors.csv`.

---

## 2. Company Contact Scraper

Scrapes company contact details (emails, phones, addresses, social links) via Google Search + official website.

### Input
JSON or CSV with fields: `company_name`, `country`, `sector` (also accepts `exhibitor_name` from GITEX output).

### Output
- **JSON** and **CSV** with: company_name, country, sector, website, emails, phones, address, social_links, source

### Setup
```bash
cd web-scrapping
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

### Usage
```bash
# From JSON
python company_contact_scraper.py companies_sample.json -o company_contacts

# From GITEX CSV (uses exhibitor_name as company_name)
python company_contact_scraper.py gitex_exhibitors.csv -o gitex_contacts

# Limit to first 5 companies (for testing)
python company_contact_scraper.py companies_sample.json --max 5

# Use DuckDuckGo instead of Google (avoids CAPTCHA; recommended)
python company_contact_scraper.py companies_sample.json --duckduckgo

# Do not block images/ads (slower but more realistic)
python company_contact_scraper.py companies_sample.json --no-block
```

Output: `company_contacts.json` and `company_contacts.csv`.

**Note:** Google often shows CAPTCHA for automated traffic. Use `--duckduckgo` to bypass; the script also auto-falls back to DuckDuckGo when CAPTCHA is detected.

---

## 3. Company Contact Enrichment (Google, Non-Headless)

Runs Chromium in **visible mode** for minimal CAPTCHA risk. Pauses for manual solve when reCAPTCHA is detected.

### Features
- Non-headless browser (en-IN locale, realistic viewport)
- Manual CAPTCHA pause: solve in browser, press Enter to continue
- Knowledge panel extraction
- Confidence scoring for website selection
- CAPTCHA encounter logging to `captcha_encounters.json`

### Usage
```bash
# Process companies (browser window will open)
python company_contact_enrichment.py companies_sample.json -o enrichment_results

# Limit to first 2 companies
python company_contact_enrichment.py companies_sample.json --max 2
```

Output: `enrichment_results.json`, `enrichment_results.csv`, `enrichment.log`
