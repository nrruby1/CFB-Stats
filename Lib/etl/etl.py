import logging
from abc import ABC, abstractmethod
from typing import Callable

from pydantic_mongo import AbstractRepository
from db.db_connection import DbConnection
from cfbd_connection import CfbdConnection

class EtlBase(ABC):
    """
    Abstract ETL tool for moving data from the CFBD API to the DB.
    """

    @abstractmethod
    def __init__(self, log: logging.Logger, *, clean_extract: bool = True, clean_staging: bool = True):
        """
        Implementations must set 'extract_datasets' and 'datasets' variables. 
        """
        self.extract_datasets: set[ExtractionDataSet] = set()
        self.datasets: set[DataSet] = set()
        self.log = log
        self.clean_extract = clean_extract
        self.clean_staging = clean_staging
    
    def run_etl(self):
        self.log.info("Running ETL tool")
        self.calc_extraction_datasets()

        # CFBD -> Extraction DB
        extract_success = self.extract()
        if not extract_success:
            self.cleanup_extraction()
            return
        
        # Extraction DB -> Staging DB
        transform_success = self.transform()
        if not transform_success:
            self.cleanup_extraction()
            self.cleanup_staging()
            return

        self.cleanup_extraction()
        
        post_transform_success = self.post_transform()
        if not post_transform_success:
            self.cleanup_staging()
            return

        validated = self.validate()
        if not validated:
            self.cleanup_staging()
            return

        # Staging DB to Presentation DB
        self.load()

        self.cleanup_staging()

        self.log.info("Finished running ETL tool")

    def extract(self) -> bool:
        """
        Extracts datasets from CFBD to the extraction DB.
        """
        self.log.info("Starting extraction of %i datasets" % len(self.extract_datasets))
        self.calc_extraction_datasets()

        with CfbdConnection(self.log) as cfbd_client,  DbConnection(self.log) as db_client:
            count = 0
            for ds in self.extract_datasets:
                count += 1
                self.log.info(f"Extracting {type(ds).__name__} ({count}/{len(self.extract_datasets)})")
                success = ds.extract(cfbd_client, db_client)
                if not success:
                    return False

        self.log.info("Finished extraction")
        return True

    def transform(self) -> bool:
        """
        Transforms extraction data and loads it into the staging DB.
        """
        self.log.info("Starting transformation of %i datasets" % len(self.datasets))

        with DbConnection(self.log) as db_client:
            count = 0
            for ds in self.datasets:
                count += 1
                self.log.info(f"Transforming {type(ds).__name__} ({count}/{len(self.datasets)})")
                success = ds.transform(db_client)
                if not success:
                    return False
                
        self.log.info("Finished transformation")
        return True

    @abstractmethod
    def post_transform(self) -> bool:
        """
        Perform additional transformations after the Dataset.
        """
        pass

    @abstractmethod
    def validate(self) -> bool:
        """
        Validate data in the staging DB before loading into production.
        """
        pass

    def cleanup_extraction(self):
        """
        Cleans up datasets in the extraction DB.
        """
        if not self.clean_extract:
            self.log.info("Skipping extraction cleanup")
            return
        
        self.log.info("Starting extraction cleanup of %i datasets" % len(self.extract_datasets))
        self.calc_extraction_datasets()
        
        with DbConnection(self.log) as db_client:
            count = 0
            for ds in self.extract_datasets:
                count += 1
                self.log.info(f"Cleaning up {type(ds).__name__} ({count}/{len(self.extract_datasets)})")
                ds.cleanup(db_client)

        self.log.info("Finished extraction cleanup")

    def load(self):
        """
        Loads data from the staging DB into the production DB.
        """
        self.log.info("Starting loading of %i datasets" % len(self.datasets))

        with DbConnection(self.log) as db_client:
            count = 0
            for ds in self.datasets:
                count += 1
                self.log.info(f"Loading {type(ds).__name__} ({count}/{len(self.datasets)})")
                ds.load(db_client)
                
        self.log.info("Finished loading")

    def cleanup_staging(self):
        """
        Cleans up datasets in the staging DB.
        """
        if not self.clean_staging:
            self.log.info("Skipping staging cleanup")
            return
        
        self.log.info("Starting staging cleanup of %i datasets" % len(self.datasets))
        
        with DbConnection(self.log) as db_client:
            count = 0
            for ds in self.datasets:
                count += 1
                self.log.info(f"Cleaning up {type(ds).__name__} ({count}/{len(self.datasets)})")
                ds.cleanup(db_client)

        self.log.info("Finished extraction cleanup")

    def calc_extraction_datasets(self):
        if len(self.extract_datasets) > 0:
            return
        
        for ds in self.datasets:
            self.extract_datasets.update(ds.extract_datasets)

class DataSet(ABC):
    """
    Represents a transformational pipeline from the extraction DB to the staging and 
    presentation DBs. Contains logic on how to transform data from extraction into 
    staging and load it into presentation.
    """

    def __init__(self, log: logging.Logger):
        self.log = log
        self.extract_datasets: set[ExtractionDataSet] = set()
    
    @abstractmethod
    def transform(self, db_client: DbConnection) -> bool:
        pass

    @abstractmethod
    def load(self, db_client: DbConnection):
        pass

    @abstractmethod
    def cleanup(self, db_client: DbConnection):
        pass

class ExtractionDataSet(ABC):
    """
    Represents a pipeline from CFBD to the extraction DB. Contains logic on how to 
    transfer data to and cleanup the extraction DB. Implementations should not 
    depend on other implementions.
    """

    def __init__(self, log: logging.Logger):
        self.log = log

    @abstractmethod
    def extract(self, cfbd_client: CfbdConnection, db_client: DbConnection) -> bool:
        pass

    @abstractmethod
    def cleanup(self, db_client: DbConnection):
        pass

def validate_mandatory_fields(entity, *fields) -> bool:
    for field in fields:
        if field not in entity or entity[field] is None:
            return False
        
        if type(entity[field]) is str and entity[field] == "":
            return False
        
        if type(entity[field]) in (dict, list, set, tuple) and len(entity[field]) == 0:
            return False
                
    return True

def load_into_production(prod_repo: AbstractRepository, stage_repo: AbstractRepository, query: Callable, replace: bool = False):
    for entity in stage_repo.find_by({}):
        prod_entity = prod_repo.find_one_by(query(entity))
        if prod_entity is not None:
            if replace:
                prod_repo.delete(prod_entity)
            else:
                continue

        prod_repo.save(entity)