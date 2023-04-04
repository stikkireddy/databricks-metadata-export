import functools
from dataclasses import dataclass
from typing import List

from delta import DeltaTable
from pyspark.sql import SparkSession

from databricks_export import BaseData, get_http_session, debug
from databricks_export.buffer import ExportBufferManager


@dataclass
class ClusterEvents(BaseData):
    timestamp: int
    type: str
    cluster_id: str


class ClusterEventsHandler:

    def __init__(self, spark: SparkSession,
                 target_table_location: str,
                 host: str,
                 token: str,
                 cluster_ids: List[str],
                 workspace_name: str = "undefined",
                 jobs_only: bool = True,
                 buffer_size=10000):
        self._cluster_ids = cluster_ids
        self._jobs_only = jobs_only
        self._spark = spark
        self._target_table_location = target_table_location
        self._host = host
        self._token = token
        self._workspace_name = workspace_name
        self._buffer_size = buffer_size

    def create_table(self):
        self._spark.createDataFrame([], ClusterEvents.to_struct_type()) \
            .write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .save(self._target_table_location)

    @functools.lru_cache(maxsize=16)
    def get_cluster_info(self, cluster_id):
        session = get_http_session()
        api = f'{self._host.rstrip("/")}/api/2.0/clusters/get'
        api_params = {
            "cluster_id": cluster_id,
        }
        api_auth = {"Authorization": f"Bearer {self._token}"}
        resp = session.get(api, json=api_params, headers=api_auth)
        return resp.json(), resp.status_code

    def does_cluster_exist(self, cluster_id):
        cluster_json, status_code = self.get_cluster_info(cluster_id)
        if 400 <= status_code <= 420:
            debug(f"Error for cluster: {cluster_json} error -> {cluster_json}")
            return False
        return True

    def is_cluster_valid(self, cluster_id):
        cluster_json, status_code = self.get_cluster_info(cluster_id)
        if 400 <= status_code <= 420:
            debug(f"Error for cluster: {cluster_json} error -> {cluster_json}")
            return False
        if self._jobs_only:
            return cluster_json["cluster_source"] in [
                "JOB", "SQL", "MODELS", "PIPELINE", "PIPELINE_MAINTAINANCE"
            ]
        return True

    def run(self):
        if DeltaTable.isDeltaTable(self._spark, self._target_table_location) is False:
            self.create_table()
        tgt = DeltaTable.forPath(self._spark, self._target_table_location)
        with ExportBufferManager("Cluster events Buffer", self._spark, tgt,
                                 ["timestamp", "type", "cluster_id",
                                  ClusterEvents.workspace_url_key()], max_buffer_size=self._buffer_size) as buf:
            for cluster_id in self._cluster_ids:
                for r in self.cluster_events_iter(cluster_id):
                    data = ClusterEvents.from_api_to_dict(r, self._workspace_name, self._host.rstrip("/"))
                    buf.add_one(data)  # buffers n records and merges into

    def cluster_events_iter(self, cluster_id):
        session = get_http_session()
        if self.does_cluster_exist(cluster_id) is False:
            debug(f"Cluster: {cluster_id} is most probably missing.")
            return {
                "timestamp": -1,  # -1 for any missing data
                "type": "MISSING_DATA",  # MISSING_DATA is not a valid event
                "cluster_id": cluster_id  # the cluster id should be passed along
            }
        if self.is_cluster_valid(cluster_id) is False:
            debug(f"Cluster: {cluster_id} is not valid.")
            return
        api = f'{self._host.rstrip("/")}/api/2.0/clusters/events'
        api_params = {
            "cluster_id": cluster_id,
            "limit": 50,
            "order": "DESC"
        }
        api_auth = {"Authorization": f"Bearer {self._token}"}
        resp = session.post(api, json=api_params, headers=api_auth).json()
        for event in resp.get("events", []):
            yield event
        # Call the API and retrieve the data
        next_page = resp.get("next_page", None)
        while next_page is not None:
            resp = session.post(api, json=next_page, headers=api_auth).json()
            for event in resp.get("events", []):
                yield event
            next_page = resp.get("next_page", None)
