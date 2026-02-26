import re
from dataclasses import dataclass
from typing import List, Tuple, Dict

@dataclass
class StateScheduleConfig:
    """Blueprint for state-specific extraction rules."""
    state_code: str
    default_walls: Tuple[int, int, int]
    footer_strings: List[str]
    
    # Regex patterns specific to the state
    series_id_pattern: re.Pattern
    legal_citation_pattern: re.Pattern
    
    # Keywords for the table parser to look for mapping columns
    header_keywords: Dict[str, List[str]]
    
    # Strings that indicate a bad extraction if found in a title
    citation_penalty_strings: List[str]