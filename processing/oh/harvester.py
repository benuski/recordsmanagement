import json
import time
import requests
import logging
import argparse
from pathlib import Path
from bs4 import BeautifulSoup

# Configure standard logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
            time.sleep(2)
        except Exception as e:
            logger.error(f"Failed to fetch page {page}: {e}")

    logger.info(f"Harvest complete! Found {len(schedule_links)} unique schedule links.")
    return schedule_links

def download_detail_pages(urls: list[str], output_dir: Path) -> None:
    """Downloads HTML files for the gathered URLs, skipping existing ones."""
    output_dir.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    for i, url in enumerate(urls):
        record_id = url.split('/')[-1]
        file_path = output_dir / f"{record_id}.html"
        
        if file_path.exists():
            continue
            
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
                
            if (i + 1) % 50 == 0 or i == 0:
                logger.info(f"[{i+1}/{len(urls)}] Downloaded record {record_id}...")
            
            time.sleep(1) 
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            time.sleep(5)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Harvest Ohio HTML retention schedules.")
    parser.add_argument("--output-directory", required=True, type=Path, help="Directory to save the raw HTML files")
    args = parser.parse_args()

    base_ohio_url = "https://rims.das.ohio.gov"
    
    # 1. Harvest the links directly into memory
    urls = harvest_links(base_ohio_url)
    
    # 2. Download the HTML pages
    if urls:
        download_detail_pages(urls, args.output_directory)