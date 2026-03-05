"""
Parse Texas agencies.html to extract agency names and metadata.
"""
import re
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime


def parse_agencies_html(html_path: Path) -> dict:
    """
    Parse agencies.html and return a dict mapping schedule_id to agency info.

    Returns:
        dict: {schedule_id: {name, approval_date, next_recert}}
    """
    agencies = {}

    with open(html_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')

    # Find all tables
    tables = soup.find_all('table')

    for table in tables:
        rows = table.find_all('tr')

        for row in rows[1:]:  # Skip header row
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue

            # First cell has "Agency Name(code)"
            agency_cell = cells[0].get_text(strip=True)

            # Extract name and code
            match = re.match(r'(.+?)\((\d{3,4})\)', agency_cell)
            if match:
                agency_name = match.group(1).strip()
                schedule_id = match.group(2)

                # Extract dates if available
                approval_date = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                date_amended = cells[2].get_text(strip=True) if len(cells) > 2 else ''
                next_recert = cells[3].get_text(strip=True) if len(cells) > 3 else ''

                # Convert approval_date to YYYY-MM-DD format
                last_updated = ''
                if approval_date:
                    try:
                        date_obj = datetime.strptime(approval_date, '%Y-%m-%d')
                        last_updated = date_obj.strftime('%Y-%m-%d')
                    except:
                        pass

                # Convert next_recert to YYYY-MM format
                next_update = ''
                if next_recert:
                    try:
                        # Format is YYYY-MM
                        if re.match(r'\d{4}-\d{2}', next_recert):
                            next_update = next_recert
                    except:
                        pass

                agencies[schedule_id] = {
                    'name': agency_name,
                    'last_updated': last_updated,
                    'next_update': next_update
                }

    return agencies


if __name__ == '__main__':
    # Test
    agencies = parse_agencies_html(Path('processing/tx/src/agencies.html'))
    print(f"Parsed {len(agencies)} agencies")

    # Show a few examples
    for code in ['356', '458', '601']:
        if code in agencies:
            print(f"{code}: {agencies[code]}")