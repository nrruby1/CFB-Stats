from pydantic_mongo import AbstractRepository
from pymongo.collection import Collection

from .db_connection import *
from .model.cfb_model import CfbBaseModel

def cleanup_collection(coll: Collection = None, repo: AbstractRepository = None):
    if coll is not None:
        coll.delete_many({})
    elif repo is not None: 
        repo.get_collection().delete_many({})
    else:
        raise Exception("Either 'coll' or 'repo' arument must not be None")

def cleanup_extraction_collections(db_client: DbConnection = DbConnection(), *colls):
    """Cleans up extraction collections. If no collections are given, all extraction collections will be cleaned up."""
    if len(colls) != 0:
        colls_to_cleanup = colls
    else:
        colls_to_cleanup = set(ExtractionCollections)
    
    for coll in colls_to_cleanup:
        if not isinstance(coll, ExtractionCollections):
            raise Exception("Given argument is not an extraction collection")
        
        cleanup_collection(coll=db_client.get_cfb_collection(Databases.extraction, coll))

def cleanup_staging_collections(db_client: DbConnection = DbConnection(), *models):
    """Cleans up staging collections. If no models are given, all staging collections will be cleaned up."""
    if len(models) != 0:
        models_to_cleanup = models
    else:
        models_to_cleanup = cfb_models
    
    for model in models_to_cleanup:
        if not issubclass(model, CfbBaseModel):
            raise Exception("Given argument is not an entity model")
        
        cleanup_collection(coll=db_client.get_cfb_collection(Databases.staging, model))

def cleanup_production_collections(db_client: DbConnection = DbConnection(), *colls):
    """Cleans up production collections. If no models are given, all production collections will be cleaned up."""
    if len(colls) != 0:
        models_to_cleanup = colls
    else:
        models_to_cleanup = cfb_models
    
    for model in models_to_cleanup:
        if not issubclass(model, CfbBaseModel):
            raise Exception("Given argument is not an entity model")
        
        cleanup_collection(coll=db_client.get_cfb_collection(Databases.production, model))