import re
from processing.base_config import StateScheduleConfig

nc_config = StateScheduleConfig(
    state_code="nc",
    default_walls=(0,0,0),
    footer_strings=[],
    series_id_pattern=re.compile(r'^\d+\.[A-Z0-9]+$'),
    legal_citation_pattern=re.compile(r'(\bG\.S\.\s*§\s*[\d\.-]+|\b\d+\s*CFR\s+[\d\.-]+|\bAuthority\s+[\w\s\(\),]+)$', re.IGNORECASE),
    header_keywords={},
    citation_penalty_strings=[]
)
