import time
import random
import sys
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
            
            if response.status_code == 429:
                logger.critical(f"Received 429 Too Many Requests. Exiting to prevent IP block.")
                sys.exit(1)
                
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/Schedule/Details/' in href:
                    full_url = base_url + href
                    if full_url not in schedule_links:
                        schedule_links.append(full_url)
            time.sleep(random.uniform(4.0, 6.0))
        except SystemExit:
            raise
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

            if response.status_code == 429:
                logger.critical(f"Received 429 Too Many Requests on {url}. Exiting to prevent IP block.")
                sys.exit(1)

            if response.status_code == 304:
                logger.info(f"General schedule {filename} is up-to-date")
                continue

            response.raise_for_status()

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)

            logger.info(f"Downloaded general schedule: {filename}")
            time.sleep(random.uniform(4.0, 6.0))
        except SystemExit:
            raise
        except Exception as e:
            logger.error(f"Error downloading general schedule {url}: {e}")
            time.sleep(random.uniform(8.0, 12.0))

def download_detail_pages(urls: list[str], output_dir: Path) -> None:
    """Downloads HTML files, utilizing If-Modified-Since to only fetch updated schedules."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use a session for better connection pooling (more human-like)
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"})

    logger.info(f"Checking/Downloading {len(urls)} Ohio records...")

    for i, url in enumerate(urls):
        record_id = url.split('/')[-1]
        file_path = output_dir / f"spec_{record_id}.html"

        request_headers = {}

        if file_path.exists():
            mtime = file_path.stat().st_mtime
            http_date = formatdate(timeval=mtime, localtime=False, usegmt=True)
            request_headers["If-Modified-Since"] = http_date

        try:
            # Randomize the delay between 6 and 10 seconds for maximum safety
            time.sleep(random.uniform(6.0, 10.0))

            # Periodically take a "long break" to look like a human
            if i > 0 and i % 100 == 0:
                logger.info("Taking a human-like break (2-3 minutes)...")
                time.sleep(random.uniform(120.0, 180.0))

            response = session.get(url, headers=request_headers, timeout=30)

            if response.status_code == 429:
                logger.critical(f"Received 429 Too Many Requests on {url}. EXITING IMMEDIATELY to protect IP.")
                sys.exit(1)

            if response.status_code == 304:
                continue

            response.raise_for_status()

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)

            if (i + 1) % 25 == 0 or i == 0:
                logger.info(f"[{i+1}/{len(urls)}] Synced record {record_id}...")

        except SystemExit:
            raise
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            # Cool down even longer after an error
            time.sleep(random.uniform(30.0, 60.0))