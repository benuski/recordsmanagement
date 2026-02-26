import json
import time
import requests
from pathlib import Path

def download_detail_pages(url_file: Path, output_dir: Path):
    with open(url_file, 'r', encoding='utf-8') as f:
        urls = json.load(f)
        
    # Create the directory to hold all 10,000+ HTML files
    output_dir.mkdir(parents=True, exist_ok=True)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    print(f"Found {len(urls)} URLs to process.")
    
    for i, url in enumerate(urls):
        # Extract the unique ID from the end of the URL (e.g., .../Details/38916)
        record_id = url.split('/')[-1]
        file_path = output_dir / f"{record_id}.html"
        
        # Skip if we already downloaded it (allows you to pause and resume!)
        if file_path.exists():
            continue
            
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
                
            # Log progress
            if (i + 1) % 50 == 0 or i == 0:
                print(f"[{i+1}/{len(urls)}] Downloaded record {record_id}...")
            
            # Be polite to the state servers (1 second delay)
            time.sleep(1) 
            
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            time.sleep(5) # Back off for 5 seconds if the server drops connection

if __name__ == '__main__':
    # Using the file you just created
    url_list_path = Path("ohio_detail_urls.json")
    
    # Save them in a dedicated raw HTML folder
    html_output_path = Path("../oh/ohio_specific")
    
    download_detail_pages(url_list_path, html_output_path)