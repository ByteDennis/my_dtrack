# dtrack tools dbx: operator helpers; the pipeline commands never import them
#
#   dtrack tools dbx login      [--profile corp] [--no-open]                 identity
#   dtrack tools dbx compute    status|start|stop --cluster-id ID [--wait] [--timeout 20]
#   dtrack tools dbx warehouse  status|start|stop --warehouse-id ID [--wait] [--timeout 20]
#   dtrack tools dbx sql        "SELECT ..." | --file q.sql --warehouse-id ID [--limit 20]
#                         one statement; several: wrap them in BEGIN ... END
#   dtrack tools dbx runs       [--limit 10] [--job-id N] [--active] | --run-id N
#   dtrack tools dbx workspace upload SOURCE [TARGET] [--overwrite] [--dry-run] [--profile corp]
#   dtrack tools dbx workspace download SOURCE [TARGET] [--overwrite] [--dry-run] [--profile corp]
#   dtrack tools dbx s3 creds   [--profile corp] [--target corp-winscp] [--watch] [--every 1200]
#   dtrack tools dbx s3 winscp  [--site corp-s3] [--profile] [--target] [--every]
#   dtrack tools dbx s3 whoami  [--profile corp]
#
# Environment (.env works): DTRACK_DBX_PROFILE (~/.databrickscfg, default corp),
#   DTRACK_DBX_CLUSTER_ID, DTRACK_DBX_WAREHOUSE_ID; DTRACK_AWS_PROFILE (default corp),
#   DTRACK_AWS_TARGET (default corp-winscp, never the source profile), DTRACK_WINSCP_SITE,
#   DTRACK_WINSCP (WinSCP.exe path). Databricks commands need dtrack[dbx]; s3 needs only boto3.

from .dbx import dbx_group
from .s3 import s3_group

dbx_group.add_command(s3_group)

__all__ = ["dbx_group"]
