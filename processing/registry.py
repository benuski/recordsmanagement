from processing.va.config import virginia_config
from processing.nc.config import nc_config
from processing.tx.config import texas_config
from processing.oh.config import ohio_config
from processing.al.config import alabama_config

# Import specific runners for states that don't follow the standard extractor_engine pattern
# We'll transition these over time.
from processing.tx.processor import run as tx_run
from processing.oh.processor import run as oh_run
from processing.nc.processor import run as nc_run
from processing.al.processor import run as al_run

STATE_REGISTRY = {
    'va': {
        'config': virginia_config,
        'runner': None, # Uses standard run_state_pipeline
    },
    'tx': {
        'config': texas_config,
        'runner': tx_run,
    },
    'oh': {
        'config': ohio_config,
        'runner': oh_run,
    },
    'nc': {
        'config': nc_config,
        'runner': nc_run,
    },
    'al': {
        'config': alabama_config,
        'runner': al_run,
    }
}
