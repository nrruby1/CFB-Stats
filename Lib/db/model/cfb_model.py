from typing import Type
from abc import ABC, abstractmethod
from pydantic import BaseModel
from pydantic_mongo import AbstractRepository, PydanticObjectId

class CfbBaseModel(ABC, BaseModel):
    
    id: PydanticObjectId = None

    @staticmethod
    @abstractmethod
    def model_id() -> str:
        return ''
    
    @staticmethod
    @abstractmethod
    def model_repository() -> Type[AbstractRepository]:
        pass