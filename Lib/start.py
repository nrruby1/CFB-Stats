import logging
import logging_config
from etl.etl_init import EtlInit

EtlInit().run_etl()

logging.shutdown()