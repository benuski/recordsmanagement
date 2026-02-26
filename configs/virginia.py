import re
from configs.base_config import StateScheduleConfig

virginia_config = StateScheduleConfig(
    state_code="va",
    default_walls=(150, 400, 550),
    footer_strings=[
        "800 e. broad", "23219", "692-3600",
        "records retention and disposition", "effective schedule date"
    ],
    # Virginia specifically uses 6-digit series numbers
    series_id_pattern=re.compile(r'^\d{6}$'),
    # Virginia specific administrative and legal codes
    legal_citation_pattern=re.compile(
        r'(\b\d+\s*CFR.*|\b\d+\s*VAC.*|\bCode of Virginia\b.*|\bCOV\b.*|\b\d+\s*USC.*)$', 
        re.IGNORECASE
    ),
    header_keywords={
        'desc': ["DESCRIPTION", "SERIES AND DESCRIPTION"],
        'id': ["NUMBER", "SERIES NUMBER"],
        'ret': ["RETENTION", "SCHEDULED RETENTION PERIOD"],
        'disp': ["DISPOSITION", "DISPOSITION METHOD"]
    },
    citation_penalty_strings=["COV", "CFR", "VAC"]
)