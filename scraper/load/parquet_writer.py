from dataclasses import dataclass
import pandas as pd
from typing import Literal, List
from gcsfs import GCSFileSystem
import os
from pandas.api.types import is_integer_dtype
import logging
import pyarrow as pa
import re

from .writer import Writer


@dataclass
class ParquetWriter(Writer):
    bucket: str
    target: Literal["local", "gcsfs"] = "local"

    def __post_init__(self):
        if self.target == "gcsfs":
            self.fs = GCSFileSystem()
        self.max_shard_limit: int = 4096
        self.write_kwargs = {
            "index": False,
            "compression": "snappy",
            "basename_template": "guid-{i}.parquet",
            "existing_data_behavior": "overwrite_or_ignore",
            "coerce_timestamps": "us",
            "allow_truncated_timestamps": True,
            "max_partitions": self.max_shard_limit,
        }

    def _local_write(
        self, df: pd.DataFrame, filename: str, partition_cols: List[str]
    ) -> None:
        path = os.path.join(self.bucket, filename)
        folder = os.path.dirname(path)
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        kwargs = {**self.write_kwargs, "partition_cols": partition_cols}
        if partition_cols:
            kwargs["partition_cols"] = partition_cols
        df.to_parquet(path, **kwargs)

    def _gcsfs_write(
        self, df: pd.DataFrame, filename: str, partition_cols: List[str]
    ) -> None:
        path = self.fs.sep.join([self.bucket, filename])
        kwargs = {**self.write_kwargs, "partition_cols": partition_cols}
        if partition_cols:
            kwargs["partition_cols"] = partition_cols
        df.to_parquet("gs://" + path, **kwargs)

    def _write(
        self, df: pd.DataFrame, filename: str, partition_cols: List[str]
    ) -> None:
        if self.target == "local":
            self._local_write(df, filename, partition_cols)
        else:
            self._gcsfs_write(df, filename, partition_cols)

    def execute(
        self, df: pd.DataFrame, filename: str, partition_cols: List[str] = []
    ) -> None:
        log_string = f"Start writing dataframe to {self.target} "
        log_string += "bucket" if self.target == "gcsfs" else "folder"
        log_string += f" {self.bucket}/{filename}"
        logging.info(log_string)
        for c in partition_cols:
            if is_integer_dtype(df[c]):
                df[c] = df[c].fillna(-1)
            else:
                df[c] = df[c].fillna("<null>")
        try:
            self._write(df, filename, partition_cols)
        except pa.lib.ArrowInvalid as e:
            error_message = str(e)
            limits = re.findall("[0-9]+", error_message)
            if len(limits) != 2:
                raise e
            new_limit, old_limit = list(map(int, limits))
            logging.warning(f"Temporarily setting max_partitions to {new_limit}")
            self.max_shard_limit = new_limit
            self.write_kwargs["max_partitions"] = new_limit
            self._write(df, filename, partition_cols)
            self.max_shard_limit = old_limit
            self.write_kwargs["max_partitions"] = old_limit
        finally:
            return
