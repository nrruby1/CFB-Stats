import logging
from Lib.etl.etl import EtlBase

log = logging.getLogger('CfbStats.etl')

class EtlSeasonStart(EtlBase):
    """
    ETL implementation for season start data.
    """
    def __init__(self, *, clean_extract: bool = True, clean_staging: bool = True):
        super().__init__(clean_extract=clean_extract, clean_staging=clean_staging)
        # TODO: Initialize self.datasets and self.extract_datasets as needed

    def post_transform(self) -> bool:
        return True

    def validate(self) -> bool:
        return True
