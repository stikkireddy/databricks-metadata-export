import functools
import json
import os
import typing
from dataclasses import dataclass
from typing import Optional, Dict, Any

import requests
from pyspark.sql.types import StructField, LongType, StringType, StructType
from requests.adapters import HTTPAdapter
from urllib3 import Retry


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



def get_retry_class(max_retries):
    class LogRetry(Retry):
      """
         Adding extra logs before making a retry request
      """
      def __init__(self, *args, **kwargs):
        if kwargs.get("total", None) != max_retries and kwargs.get("total", None) > 0:
            print(f'Retrying with kwargs: {kwargs}')
        super().__init__(*args, **kwargs)

    return LogRetry

@functools.lru_cache(maxsize=None)
def get_http_session():
    s = requests.Session()
    max_retries = int(os.getenv("DATABRICKS_REQUEST_RETRY_COUNT", 10))
    retries = get_retry_class(max_retries)\
        (total=max_retries, backoff_factor=1, status_forcelist=[500, 501, 502, 503, 504, 429])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s