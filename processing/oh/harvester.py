import time
import requests
import logging
from pathlib import Path
from bs4 import BeautifulSoup
from email.utils import formatdate

logger = logging.getLogger(__name__)

def harvest_links(base_url: str) -> list[str]:
    """Scrapes the search result pages to find all individual schedule URLs."""
    schedule_links = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    # Pages 1 to 3 at 5000 items per page covers the ~10,500 items
    for page in range(1, 4):
        logger.info(f"Fetching search results page {page}...")
        url = f"{base_url}/Schedule?Page={page}&PageSize=5000"
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/Schedule/Details/' in href:
                    full_url = base_url + href
                    if full_url not in schedule_links:
                        schedule_links.append(full_url)
            time.sleep(5)
        except Exception as e:
            logger.error(f"Failed to fetch page {page}: {e}")

    logger.info(f"Harvest complete! Found {len(schedule_links)} unique schedule links.")
    return schedule_links

def download_general_schedule(base_url: str, output_dir: Path) -> None:
    """Downloads the Ohio General Records Retention Schedule page."""
    output_dir.mkdir(parents=True, exist_ok=True)
    base_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    # Ohio has 3 general schedule pages
    general_urls = [
        (f"{base_url}/Schedule", "gen_1.html"),
        (f"{base_url}/Schedule?Function=Repository", "gen_repository.html"),
        (f"{base_url}/Schedule?Function=Specific", "gen_specific.html"),
    ]

    for url, filename in general_urls:
        file_path = output_dir / filename
        request_headers = base_headers.copy()

        # Check if file exists and use If-Modified-Since
        if file_path.exists():
            mtime = file_path.stat().st_mtime
            http_date = formatdate(timeval=mtime, localtime=False, usegmt=True)
            request_headers["If-Modified-Since"] = http_date

        try:
            response = requests.get(url, headers=request_headers)

            if response.status_code == 304:
                logger.info(f"General schedule {filename} is up-to-date")
                continue

            response.raise_for_status()

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)

            logger.info(f"Downloaded general schedule: {filename}")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error downloading general schedule {url}: {e}")
            time.sleep(10)

def download_detail_pages(urls: list[str], output_dir: Path) -> None:
    """Downloads HTML files, utilizing If-Modified-Since to only fetch updated schedules."""
    output_dir.mkdir(parents=True, exist_ok=True)
    base_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    for i, url in enumerate(urls):
        record_id = url.split('/')[-1]
        file_path = output_dir / f"spec_{record_id}.html"  # Prefix with spec_ for clarity

        request_headers = base_headers.copy()

        # If we already have the file, ask the server if it has been modified since we last downloaded it
        if file_path.exists():
            mtime = file_path.stat().st_mtime
            # Format the local file's modification time into the standard HTTP date format
            http_date = formatdate(timeval=mtime, localtime=False, usegmt=True)
            request_headers["If-Modified-Since"] = http_date

        try:
            response = requests.get(url, headers=request_headers)

            # 304 Not Modified means our local copy is still perfectly up-to-date
            if response.status_code == 304:
                continue

            response.raise_for_status()

            # If we get a 200 OK, the file is new or updated, so we write/overwrite it
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)

            if (i + 1) % 50 == 0 or i == 0:
                logger.info(f"[{i+1}/{len(urls)}] Downloaded new or updated record {record_id}...")

            time.sleep(5)
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            time.sleep(10)