import logging
import logging_config
from db.db_cleanup import *
from etl.etls.etl_init import EtlInit
from etl.etls.etl_season_start import EtlSeasonStart
from etl.etls.etl_weekly_results import EtlWeeklyResults

with DbConnection(True) as db_client:
    cleanup_extraction_collections(db_client)
    cleanup_staging_collections(db_client)
    cleanup_production_collections(db_client)

EtlInit(
    years=[2024], test_mode=True, clean_extract=False, clean_staging=False
).run_etl()

# EtlSeasonStart(
#     years=[2024],
#     test_mode=True,
#     clean_extract=False,
#     clean_staging=False,
# ).run_etl()

# EtlWeeklyResults(
#     year=2024, week=1, test_mode=True, clean_extract=False, clean_staging=False
# ).run_etl()

logging.shutdown()
