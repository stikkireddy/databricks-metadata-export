import json
import typing
from dataclasses import dataclass
from typing import Optional, Dict, Any

from pyspark.sql.types import StructField, LongType, StringType, StructType


def is_optional(field):
    return typing.get_origin(field) is typing.Union and \
           type(None) in typing.get_args(field)


@dataclass
class BaseData:

    @staticmethod
    def raw_data_key():
        return "_raw_data"

    @staticmethod
    def workspace_name_key():
        return "_workspace_name"

    @staticmethod
    def workspace_url_key():
        return "_workspace_url"

    @classmethod
    def to_struct_type(cls):
        fields = []
        for k, v in cls.__dict__["__annotations__"].items():
            if v in [int, Optional[int]]:
                fields.append(StructField(name=k, dataType=LongType()))
            elif v in [str, Optional[str]]:
                fields.append(StructField(name=k, dataType=StringType()))
            else:
                raise Exception(f"not supported key: {k} data type {type(v)} for class: {cls.__name__}")
        fields.append(StructField(name=cls.raw_data_key(), dataType=StringType()))
        fields.append(StructField(name=cls.workspace_name_key(), dataType=StringType()))
        fields.append(StructField(name=cls.workspace_url_key(), dataType=StringType()))
        return StructType(fields=fields)

    @classmethod
    def from_api_to_dict(cls, api_json: Dict[str, Any], workspace_name: str, workspace_url: str):
        result_data: Dict[str, Any] = {}
        for k, v in cls.__dict__["__annotations__"].items():
            if k != "raw_data":
                result_data[k] = api_json.get(k, None)
        result_data[cls.raw_data_key()] = json.dumps(api_json)
        result_data[cls.workspace_name_key()] = workspace_name
        result_data[cls.workspace_url_key()] = workspace_url
        return result_data
