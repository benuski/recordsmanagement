import requests
from bs4 import BeautifulSoup
import time
import json
from pathlib import Path

def harvest_links(output_file: Path):
    base_url = "https://rims.das.ohio.gov"
    schedule_links = []

    # There are ~10,500 items, so at 5000 per page, we only need pages 1, 2, and 3.
    for page in range(1, 4):
        print(f"Fetching search results page {page}...")
        
        # Paging is handled via query parameters
        url = f"{base_url}/Schedule?Page={page}&PageSize=5000"
        
        # Add a standard user-agent so the server doesn't block the automated request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # The authorization numbers in the table link to the details page
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/Schedule/Details/' in href:
                    full_url = base_url + href
                    if full_url not in schedule_links:
                        schedule_links.append(full_url)
                        
            # Be polite to their server between requests
            time.sleep(2)
            
        except Exception as e:
            print(f"Failed to fetch page {page}: {e}")

    print(f"Harvest complete! Found {len(schedule_links)} unique schedule links.")

    # Save the links locally so we can process them in Step 2
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(schedule_links, f, indent=4)
        
    print(f"Saved links to {output_file}")

if __name__ == '__main__':
    # Save the urls to a holding file
    output_path = Path("ohio_detail_urls.json")
    harvest_links(output_path)