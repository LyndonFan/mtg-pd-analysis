from dataclasses import dataclass, field
from typing import ClassVar, Dict, Type
import pandas as pd
import logging

from .aggregator import Aggregator
from load.writer import Writer


@dataclass
class AggregateManager:
    writer: Writer
    aggregators: ClassVar[Dict[str, Type[Aggregator]]] = {}

    @classmethod
    def register(cls, name: str):
        def wrapped(subcls: Type[Aggregator]) -> Type[Aggregator]:
            if name in cls.aggregators:
                raise ValueError(
                    f"{name} is already registered: {cls.aggregators.keys()}"
                )
            cls.aggregators[name] = subcls
            return subcls

        return wrapped

    def execute(self, df: pd.DataFrame):
        logging.info(f"Found {len(self.aggregators)} registered aggregators")
        for name, agg_class in self.aggregators.items():
            logging.info(f"Aggregating df with {name} aggregator")
            aggregator = agg_class()
            agg_df = aggregator.execute(df)
            self.writer.execute(agg_df, name + ".parquet", aggregator.groupby_columns)
