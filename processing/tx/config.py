import re
from processing.base_config import StateScheduleConfig

texas_config = StateScheduleConfig(
    state_code="tx",
    # g1: end of RSIN column (usually ends ~100)
    # g2: end of Title column (~240)
    # g3: end of Description column (~440)
    # g4: end of Retention Code column (~530)
    # g5: end of Retention Period columns (~700)
    default_walls=(100, 240, 440, 530, 700),
    footer_strings=[
        "texas state library", "archives commission", "tslac",
        "records retention schedule", "state and local records"
    ],
    # Texas uses alphanumeric series IDs. We require them to start with a digit 
    # or be a specific pattern to avoid matching common words in silo mode.
    series_id_pattern=re.compile(r'^\d.*$|^[A-Z]{2,}\s*\d.*$'),
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
