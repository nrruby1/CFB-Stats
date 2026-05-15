import logging
from abc import ABC, abstractmethod
from math import e, prod
from typing import Callable, Iterable
from venv import create

from pydantic_mongo import AbstractRepository

from pymongo import InsertOne, ReplaceOne
from pymongo.client_session import ClientSession
from timer import Timer
from db.db_connection import *
from db.model.cfb_model import CfbBaseModel
from db.db_cleanup import *
from db.db_utility import *
from etl.cfbd_connection import CfbdConnection

log = logging.getLogger("CfbStats.etl.etls")


class EtlBase(ABC):
    """
    Abstract ETL tool for moving data from external sources to the DB.
    """

    @abstractmethod
    def __init__(
        self,
        name: str,
        *,
        skip_extract: bool = False,
        clean_extract: bool = True,
        clean_staging: bool = True,
        test_mode: bool = False,
    ):
        """
        Implementations must set 'extract_datasets' and 'datasets' variables.
        """
        self.name = name
        self.extract_datasets: set[ExtractionDataSet] = set()
        self.models: dict[type[CfbBaseModel], bool] = {}
        self.datasets: list[DataSet] = []
        self.skip_extract = skip_extract
        self.clean_extract = clean_extract
        self.clean_staging = clean_staging
        self.test_mode = test_mode

    def run_etl(self):
        log.info(f"Running {self.name} ETL tool")
        etl_timer = Timer(self.name)

        self.calculate_datasets()

        with CfbdConnection() as cfbd_client, DbConnection(self.test_mode) as db_client:

            # External data -> Extraction DB
            extract_success = Timer("Extraction").run(
                lambda: self.extract(cfbd_client, db_client)
            )
            if not extract_success:
                log.error("Extraction failed. Cancelling remaining ETL steps.")
                self.cleanup_extraction(db_client)
                return

            # Extraction DB -> Staging DB
            transform_success = Timer("Transformation").run(
                lambda: self.transform(db_client)
            )
            if not transform_success:
                log.error("Transformation failed. Cancelling remaining ETL steps.")
                self.cleanup_extraction(db_client)
                self.cleanup_staging(db_client)
                return

            # Additional transformations
            log.info("Running post transformation")
            post_transform_success = Timer("Post Transformation").run(
                lambda: self.post_transform(db_client)
            )

            if not post_transform_success:
                log.error("Post transformation failed. Cancelling remaining ETL steps.")
                self.cleanup_extraction(db_client)
                self.cleanup_staging(db_client)
                return

            self.cleanup_extraction(db_client)

            # Validate Staging DB
            log.info("Running validation")
            validated = Timer("Validation").run(lambda: self.validate(db_client))
            if not validated:
                log.error("Validation failed. Cancelling remaining ETL steps.")
                self.cleanup_staging(db_client)
                return

            # Staging DB to Presentation DB
            with db_client.start_session() as session:
                timer = Timer("Loading")
                session.with_transaction(lambda s: self.load(s, db_client))
                timer.stop_and_log()

            self.cleanup_staging(db_client)

        log.debug(etl_timer.stop())
        log.info(f"Finished running {self.name} ETL tool")

    def extract(self, cfbd_client: CfbdConnection, db_client: DbConnection) -> bool:
        """
        Extracts datasets from CFBD to the extraction DB.
        """
        if self.skip_extract:
            log.info("Skipping extraction")
            return True

        log.info("Running extraction for %i datasets" % len(self.extract_datasets))
        self.calculate_datasets()

        try:
            count = 0
            operations: list = []
            for ds in self.extract_datasets:
                count += 1
                log.info(
                    f"Extracting {type(ds).__name__} ({count}/{len(self.extract_datasets)})"
                )
                success = ds.extract(cfbd_client, db_client, operations)
                if not success:
                    return False

            db_client.bulk_write(operations)
        except Exception as e:
            log.exception(f"Error during extraction: {e}")
            return False

        return True

    def transform(self, db_client: DbConnection) -> bool:
        """
        Transforms extraction data and loads it into the staging DB.
        """
        log.info("Running transformation for %i datasets" % len(self.datasets))
        self.calculate_datasets()

        try:
            count = 0
            operations: list = []
            for ds in self.datasets:
                count += 1
                log.info(
                    f"Transforming {type(ds).__name__} ({count}/{len(self.datasets)})"
                )
                success = ds.transform(db_client, operations)
                if not success:
                    return False

            db_client.bulk_write(operations)
        except Exception as e:
            log.exception(f"Error during transformation: {e}")
            return False
        return True

    @abstractmethod
    def post_transform(self, db_client: DbConnection) -> bool:
        """
        Perform additional transformations after the Datasets.
        """
        pass

    @abstractmethod
    def validate(self, db_client: DbConnection) -> bool:
        """
        Validate data in the staging DB before loading into production.
        """
        log.debug("No validation configured for this ETL")

    def cleanup_extraction(self, db_client: DbConnection):
        """
        Cleans up datasets in the extraction DB.
        """
        if not self.clean_extract:
            log.info("Skipping extraction cleanup")
            return

        log.info("Running extraction cleanup")
        Timer("Extraction Cleanup").run(
            lambda: cleanup_extraction_collections(db_client)
        )

    def load(self, session: ClientSession, db_client: DbConnection):
        """
        Loads data from the staging DB into the production DB.
        """
        log.info("Running loading for %i models" % len(self.models))
        self.calculate_datasets()

        try:
            operations: list = []
            for model in self.models:
                stage_repository: AbstractRepository = db_client.get_cfb_repository(
                    Databases.staging, model
                )
                prod_repository: AbstractRepository = db_client.get_cfb_repository(
                    Databases.production, model
                )

                stage_entities: list[CfbBaseModel] = list(stage_repository.find_by({}))
                if len(stage_entities) == 0:
                    log.warning(
                        f"No entities found in staging for model {model.__name__}, skipping load"
                    )
                    continue

                prod_entities: Iterable[CfbBaseModel] = prod_repository.find_by({})

                for entity in stage_entities:
                    op = None

                    if self.models[model]:
                        op = insert_one_operation(
                            db_client=db_client,
                            db=Databases.production,
                            entity=entity,
                            do_replace=True,
                        )

                    else:
                        prod_entity = next(
                            (e for e in prod_entities if e == entity), None
                        )
                        if prod_entity is not None:
                            continue

                        op = insert_one_operation(
                            db_client=db_client,
                            db=Databases.production,
                            entity=entity,
                            do_replace=False,
                        )

                    if op is not None:
                        operations.append(op)
                    else:
                        log.warning(
                            f"Failed to create insert operation for entity {entity}"
                        )

            db_client.bulk_write(operations, session=session)
        except Exception as e:
            log.exception(f"Error during loading: {e}")

        log.info(f"Loaded {len(operations)} entities into the production DB")

    def cleanup_staging(self, db_client: DbConnection):
        """
        Cleans up datasets in the staging DB.
        """
        if not self.clean_staging:
            log.info("Skipping staging cleanup")
            return

        log.info("Running staging cleanup")
        Timer("Staging Cleanup").run(lambda: cleanup_staging_collections(db_client))

    def calculate_datasets(self):
        """
        Calculates the extraction datasets and models to be processed.

        This can be overwritten to change the override booleans for certainmodels.
        """
        if len(self.extract_datasets) > 0 and len(self.models) > 0:
            return

        self.models: dict[type[CfbBaseModel], bool] = {}
        self.extract_datasets: set[ExtractionDataSet] = set()

        for ds in self.datasets:
            for dataset in ds.extract_datasets:
                if not any(isinstance(d, type(dataset)) for d in self.extract_datasets):
                    self.extract_datasets.add(dataset)

            self.models.update(ds.models)
            for model in ds.models:
                if model not in self.models:
                    self.models[model] = ds.models[model]
                elif ds.models[model] and not self.models[model]:
                    self.models[model] = True


class DataSet(ABC):
    """
    Represents a transformational pipeline from the extraction DB to the staging and
    presentation DBs. Contains logic on how to transform data from extraction into
    staging and load it into presentation.
    """

    def __init__(self):
        """Parameters should be required and passed down from the calling ETL."""
        self.extract_datasets: set[ExtractionDataSet] = set()
        self.models: dict[type[CfbBaseModel], bool] = {}

    @abstractmethod
    def transform(self, db_client: DbConnection, operations: list) -> bool:
        pass


class ExtractionDataSet(ABC):
    """
    Represents a pipeline from CFBD to the extraction DB. Contains logic on how to
    transfer data to and cleanup the extraction DB. Implementations should not
    depend on other implementions.
    """

    def __init__(self):
        """Parameters should be required and passed down from the calling DataSet."""
        pass

    @abstractmethod
    def extract(
        self, cfbd_client: CfbdConnection, db_client: DbConnection, operations: list
    ) -> bool:
        pass
