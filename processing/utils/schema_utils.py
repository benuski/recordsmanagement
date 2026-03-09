from processing.base_config import StateScheduleConfig

def make_record(schema: dict, **overrides) -> dict:
    """Creates a new record dict, safely falling back to overrides if schema is empty."""
    if not schema:
        return overrides
        
    record = dict(schema)
    for key, value in overrides.items():
        if key in record:
            record[key] = value
    return record

def score_records(records: list[dict], config: StateScheduleConfig) -> int:
    """Evaluates the quality of a parsed dataset to determine the winner."""
    if not records:
        return -9999

    score = len(records) * 10
    seen_ids: set[str] = set()

    for r in records:
        title = r.get('series_title', '').strip()
        desc = r.get('series_description', '').strip()
        ret = r.get('retention_statement', '').strip()
        sid = r.get('series_id', '')

        if sid in seen_ids:
            score -= 20
        seen_ids.add(sid)

        if not title: score -= 15
        if not desc: score -= 5
        if not ret: score -= 10

        # Penalties: Catch horizontally merged "snowballs"
        if title.lower().startswith('this series') or title.lower().startswith('documents '):
            score -= 15
        if len(title) > 200:
            score -= 50

        if desc and not title: score -= 10

        if any(penalty in title for penalty in config.citation_penalty_strings):
            score -= 10
        if config.series_id_pattern.search(title):
            score -= 10

        if r.get('retention_years') is not None:
            score += 3

    return score
