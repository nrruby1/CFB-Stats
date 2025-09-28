import logging
from etl.etl_init import EtlInit

logging.basicConfig(filename='Log/testlog.log', 
                    format= '%(asctime)s | %(levelname)s | %(name)s - %(message)s', 
                    level=logging.INFO)

EtlInit().run_etl()

logging.shutdown()