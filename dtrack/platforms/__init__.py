"""Platform strategy pattern for data extraction across SAS, Oracle, and Athena."""

from .base import PlatformBuilder
from .oracle import OracleBuilder
from .sas import SASBuilder
from .athena import AthenaBuilder


_REGISTRY = {
    'oracle': OracleBuilder,
    'sas': SASBuilder,
    'athena': AthenaBuilder,
    'aws': AthenaBuilder,
}


def detect_platform(tbl_cfg):
    """Auto-detect platform from table config.

    Priority: explicit 'platform' > $ prefix (sas) > source field.
    """
    explicit = tbl_cfg.get('platform', '').lower()
    if explicit in _REGISTRY:
        return explicit

    source = tbl_cfg.get('source', '').lower()
    if source == 'sas':
        return 'sas'
    if source == 'oracle':
        return 'oracle'
    if source == 'aws':
        return 'athena'

    return 'oracle'


def get_builder(tbl_cfg, db_path=None):
    """Factory: return the right PlatformBuilder subclass instance."""
    platform = detect_platform(tbl_cfg)
    cls = _REGISTRY[platform]
    return cls(tbl_cfg, db_path=db_path)
