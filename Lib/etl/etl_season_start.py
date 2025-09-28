import logging
from Lib.etl.etl import EtlBase

class EtlSeasonStart(EtlBase):
    """
    ETL implementation for season start data.
    """
    def __init__(self, log: logging.Logger, *, clean_extract: bool = True, clean_staging: bool = True):
        super().__init__(log, clean_extract=clean_extract, clean_staging=clean_staging)
        # TODO: Initialize self.datasets and self.extract_datasets as needed

    def post_transform(self) -> bool:
        return True

    def validate(self) -> bool:
        return True
