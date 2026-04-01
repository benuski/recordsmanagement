import re
from processing.base_config import StateScheduleConfig

alabama_config = StateScheduleConfig(
    state_code="al",
    default_walls=(0, 0, 0), # Not used for narrative
    footer_strings=[],
    series_id_pattern=re.compile(r"^$"), # Alabama doesn't use numeric IDs in these docs
    legal_citation_pattern=re.compile(r"Code of Alabama 1975 § [\d\-a-zA-Z\s\(\),]+(?=\.|\)|$)"),
    header_keywords={},
    citation_penalty_strings=[],
    base_url="https://archives.alabama.gov/FindRDA.aspx"
)
