import pandas as pd
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import List


@dataclass
class Aggregator:
    groupby_columns: List[str]
    source_columns: List[str]

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[self.source_columns].copy()
        if "sourceName" in self.source_columns:
            df["sourceName"] = df["sourceName"].cat.add_categories("Both")
            df_copy = df.copy()
            df_copy["sourceName"] = "Both"
            df = pd.concat([df_copy, df], ignore_index=True)
        return df

    @abstractmethod
    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
