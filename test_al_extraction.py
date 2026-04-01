import json
from pathlib import Path
from processing.al.config import alabama_config
from processing.al.extractor import AlabamaNarrativeExtractor

def test():
    schema = {} # Minimal schema
    md_path = Path("processing/al/src/911_Board.md")
    with open(md_path, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    extractor = AlabamaNarrativeExtractor(alabama_config, schema)
    records = extractor.parse_markdown(md_content, "911_Board")
    
    print(f"Agency Name: {extractor.agency_name}")
    print(f"Total records: {len(records)}")
    
    print("\nBibliographic Map:")
    for k, v in extractor.bib_map.items():
        print(f"  {k} -> {v}")
        
    print("\nRecords extracted:")
    for r in records:
        print(f"Title: {r['series_metadata']['series_title']}")
        print(f"  Desc: {r['series_metadata']['series_description'][:100]}...")
        print(f"  Disp: {r['retention_rules']['disposition_method']}")
        print(f"  Ret: {r['retention_rules']['duration_years']} years")
        print(f"  Cit: {r['series_metadata']['legal_citation']}")
        print("-" * 20)

if __name__ == "__main__":
    test()
