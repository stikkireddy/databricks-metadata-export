import time
from dataclasses import dataclass
from typing import Optional

from delta import DeltaTable
from pyspark.sql import SparkSession

from databricks_export import BaseData, get_http_session
from databricks_export.buffer import ExportBufferManager


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
                 last_n_days: int = 7,
                 buffer_size=10000):
        self._spark = spark
        self._target_table_location = target_table_location
        self._host = host
        self._token = token
        self._last_n_days = last_n_days
        self._workspace_name = workspace_name
        self._buffer_size = buffer_size

    def create_table(self):
        self._spark.createDataFrame([], JobRunData.to_struct_type()) \
            .write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .save(self._target_table_location)

    def job_runs_iter(self):
        session = get_http_session()
        api = f'{self._host.rstrip("/")}/api/2.1/jobs/runs/list'
        api_params = {
            "limit": 25,
            "expand_tasks": "true",
        }
        if self._last_n_days > 0:
            print("Configuring start time")
            start_time = time.time() * 1000 - self._last_n_days * 24 * 60 * 60 * 1000
            api_params["start_time_from"] = start_time
        api_auth = {"Authorization": f"Bearer {self._token}"}
        resp = session.get(api, params=api_params, headers=api_auth).json()
        for run in resp.get("runs", []):
            yield run
        # Call the API and retrieve the data
        offset = 0
        has_more = True
        while has_more:
            api_params["offset"] = offset
            resp = session.get(api, params=api_params, headers=api_auth).json()
            for run in resp.get("runs", []):
                yield run
            offset += api_params["limit"]
            has_more = resp["has_more"]

    def run(self):
        if DeltaTable.isDeltaTable(self._spark, self._target_table_location) is False:
            self.create_table()
        tgt = DeltaTable.forPath(self._spark, self._target_table_location)
        with ExportBufferManager("Job Runs Buffer", self._spark, tgt,
                                 ["job_id", "run_id", JobRunData.workspace_url_key()],
                                 max_buffer_size=self._buffer_size) as buf:
            for r in self.job_runs_iter():
                data = JobRunData.from_api_to_dict(r, self._workspace_name, self._host.rstrip("/"))
                buf.add_one(data)  # buffers n records and merges into


class JobsTableHelper:
    def __init__(self,
                 spark: SparkSession,
                 target_table_location: str):
        self._spark = spark
        self._target_table_location = target_table_location
        self._table_path = f"delta.`{target_table_location}`"

    def _last_n_days_job_clusters_sql(self, last_n_days=7):
        start_time = time.time() * 1000 - last_n_days * 24 * 60 * 60 * 1000
        return f"""SELECT DISTINCT explode(array_distinct(filter(
          array_union(
            from_json(_raw_data:tasks[*].cluster_instance.cluster_id, "array<string>"),
            array(_raw_data:cluster_instance:cluster_id)
          )  , x -> x is not null))) as distinct_clusters 
          FROM delta.`dbfs:/tmp/sri/jobs_delta_dump_v3`
          WHERE start_time > {start_time}
        """

    def last_n_days_job_clusters(self, last_n_days=7):
        return self._spark.sql(self._last_n_days_job_clusters_sql(last_n_days))
