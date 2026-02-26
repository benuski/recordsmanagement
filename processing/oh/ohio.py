import re
from processing.base_config import StateScheduleConfig
    state_code="oh",
    # PDF parsing walls/footers are not applicable for Ohio's HTML pipeline
    default_walls=(0, 0, 0),
    footer_strings=[],
    
    # Ohio uses varied alphanumeric IDs, so we allow any non-empty string to pass
    series_id_pattern=re.compile(r'.+'),
    
    # Ohio specific administrative and federal legal codes
    legal_citation_pattern=re.compile(
        r'(\bORC\s*\d+\.\d+|\b\d+\s*CFR\s*\d+|\b\d+\s*USC\s*\d+)', 
        re.IGNORECASE
    ),
    
    # Header keywords (used lightly here as HTML tables have specific <th> tags)
    header_keywords={
        'desc': ["DESCRIPTION"],
        'id': ["NUMBER"],
        'ret': ["RETENTION PERIOD"],
        'disp': ["DISPOSITION"]
    },
    
    # Penalize extractions that accidentally make citations the title
    citation_penalty_strings=["ORC", "CFR", "USC"]
)