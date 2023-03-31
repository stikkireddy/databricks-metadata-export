from typing import List

from delta import DeltaTable
from pyspark.sql import SparkSession


class ExportBufferManager:
    # todo: mapping the primary keys
    def __init__(self, name: str, spark: SparkSession, target_table: DeltaTable, primary_keys: List[str],
                 max_buffer_size=10000,
                 debug_every_n_records=1000,
                 auto_flush=True):
        # TODO: not multi threaded dont try to use this multi threaded no locks
        self._name = name
        self._spark = spark
        self._target_table = target_table
        self._max_buffer_size = max_buffer_size
        self._buffer = []
        self._auto_flush = auto_flush
        self._primary_keys = primary_keys
        self._buffer_use_count = 0
        self._debug_every_n_records = debug_every_n_records

    def add_many(self, *elements):
        for element in elements:
            self.add_one(element)

    def _check_commit(self):
        if self._auto_flush is True and len(self._buffer) >= self._max_buffer_size:
            print(f"Committing to delta table: ct {self._buffer_use_count}")
            # TODO: error handling
            input_data = self._spark.createDataFrame(self._buffer, self._target_table.toDF().schema).alias("s") \
                .drop_duplicates(self._primary_keys)
            (self._target_table.alias("t")
             .merge(input_data,
                    " and ".join([f"t.{k} = s.{k}" for k in self._primary_keys]))
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
            # empty the buffer
            self._buffer = []

    def commit(self):
        self._check_commit()

    def add_one(self, element):
        self._check_commit()
        self._buffer.append(element)
        if self._buffer_use_count % self._debug_every_n_records == 0:
            print(f"Debug [{self._name}]: ct {self._buffer_use_count}")
        self._buffer_use_count += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        print(f"Optimizing Table: {self._name}")
        self._target_table.optimize()
