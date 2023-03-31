import json
import typing
from dataclasses import dataclass
from typing import Dict, Any, Optional

import requests
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType

from databricks_export.buffer import ExportBufferManager


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


@dataclass
class JobRunData(BaseData):
    job_id: int
    run_id: int
    creator_user_name: str
    start_time: int
    setup_duration: int
    cleanup_duration: int
    end_time: int
    run_duration: int
    trigger: str
    run_name: str
    run_page_url: str
    run_page_url: str
    attempt_number: Optional[str]


class JobRunsHandler:

    def __init__(self, spark: SparkSession,
                 target_table_location: str,
                 host: str,
                 token: str,
                 workspace_name: str = "undefined",
                 buffer_size=10000):
        self._spark = spark
        self._target_table_location = target_table_location
        self._host = host
        self._token = token
        self._workspace_name = workspace_name
        self._buffer_size = buffer_size

    def create_table(self):
        self._spark.createDataFrame([], JobRunData.to_struct_type()) \
            .write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .save(self._target_table_location)

    def job_runs_iter(self):
        api = f'{self._host.rstrip("/")}/api/2.1/jobs/runs/list'
        api_params = {
            "limit": 25,
            "expand_tasks": True
        }
        api_auth = {"Authorization": f"Bearer {self._token}"}
        resp = requests.get(api, params=api_params, headers=api_auth).json()
        for run in resp.get("runs", []):
            yield run
        # Call the API and retrieve the data
        offset = 0
        has_more = True
        while has_more:
            api_params["offset"] = offset
            resp = requests.get(api, params=api_params, headers=api_auth).json()
            for run in resp.get("runs", []):
                yield run
            offset += api_params["limit"]
            has_more = resp["has_more"]

    def run(self):
        if DeltaTable.isDeltaTable(self._spark, self._target_table_location) is False:
            self.create_table()
        tgt = DeltaTable.forPath(self._spark, self._target_table_location)
        with ExportBufferManager("Job Runs Buffer", self._spark, tgt,
                           ["job_id", "run_id"], max_buffer_size=self._buffer_size) as buf:
            for r in self.job_runs_iter():
                data = JobRunData.from_api_to_dict(r, self._workspace_name, self._host)
                buf.add_one(data)  # buffers 1000 records and merges into
