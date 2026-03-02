import time
import logging
from typing import Callable
import config
import cfbd
from cfbd.api_client import ApiClient

log = logging.getLogger('CfbStats')

class CfbdConnection(ApiClient):

    def __init__(self):
        # Configure Bearer authorization: apiKey
        configuration = cfbd.Configuration(
            host = "https://api.collegefootballdata.com",
            access_token =  config.cfbd_token
        )

        super().__init__(configuration)

    def __del__(self):
        log.debug("CFBD API client closed")
        super().close()

retries = 3
wait_time = 5

def api_call(lamb: Callable) -> list | None:

    """
    Executes a CFDB API call from a lambda. Handles exceptions with 
    a set number of retries and a timeout. This is due to the API 
    occasionally returning exceptions for no reason. 
    """
    for x in range(retries):
        try:
            val = lamb()
        except Exception as e:
            log.error(f"Exception when calling API: attempt {x}/{retries}")
            ex = e
        else:
            return val
        
        time.sleep(wait_time)

    log.exception(f"Unable to fetch from API in {retries} attempts\n{ex}")
    return None
