from typing import Optional, Type
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, ConfigDict
from pydantic_mongo import AbstractRepository, PydanticObjectId


class CfbBaseModel(ABC, BaseModel):

    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        arbitrary_types_allowed=True,
        use_enum_values=True,
    )

    id: Optional[PydanticObjectId] = Field(default=None, alias="_id")

    @property
    def _id(self) -> Optional[PydanticObjectId]:
        return self.id

    @abstractmethod
    def get_model_query(self) -> dict:
        pass

    @staticmethod
    @abstractmethod
    def model_id() -> str:
        return ""

    @staticmethod
    @abstractmethod
    def model_repository() -> Type[AbstractRepository]:
        pass
