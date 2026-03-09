import re
from processing.base_config import StateScheduleConfig

texas_config = StateScheduleConfig(
    state_code="tx",
    default_walls=(150, 400, 550),
    footer_strings=[
        "texas state library", "archives commission", "tslac",
        "records retention schedule", "state and local records"
    ],
    # Texas uses numeric series IDs with dots (e.g., 1.1.001, 4.2.015)
    series_id_pattern=re.compile(r'^\d+\.\d+\.\d+$'),
    # Texas legal citations
    legal_citation_pattern=re.compile(
        r'(\bTAC\b.*|\bTexas Administrative Code\b.*|\bGovernment Code\b.*|\b\d+\s*TAC.*)',
        re.IGNORECASE
    ),
    header_keywords={
        'desc': ["DESCRIPTION", "RECORD SERIES TITLE", "SERIES DESCRIPTION"],
        'id': ["RSIN", "SERIES ID", "RECORD SERIES"],
        'ret': ["RETENTION", "RETENTION PERIOD"],
        'disp': ["DISPOSITION", "ARCHIVAL"]
    },
    citation_penalty_strings=["TAC", "Government Code"]
)
