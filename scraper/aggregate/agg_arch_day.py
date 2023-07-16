import pandas as pd
from typing import List
import logging
from datetime import datetime

from .aggregator import Aggregator
from .aggregate_manager import AggregateManager

GROUPBY_COLUMNS = [
    "seasonId",
    "sourceName",
    "date",
    "archetypeId",
    "archetypeName",
]


@AggregateManager.register("archetype_day")
class ArchetypeDayAggregator(Aggregator):
    def __init__(self) -> None:
        # note "date" isn't a column in input df!
        super().__init__(
            GROUPBY_COLUMNS,
            [
                "seasonId",
                "personId",
                "sourceName",
                "matches",
                "archetypeId",
                "archetypeName",
                "createdDatetime",
                "updatedDatetime",
            ],
        )

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._preprocess(df)
        df = df[df["matches"] > 0].copy()
        df["updatedDatetime"] = df["updatedDatetime"].fillna(datetime.now())
        df["date"] = df.apply(
            lambda row: pd.date_range(
                row["createdDatetime"], row["updatedDatetime"]
            ).tolist(),
            axis=1,
        )
        df = df.explode("date")
        df["date"] = df["date"].dt.strftime("%Y%m%d")
        df = df.dropna(subset="date")
        df["date"] = df["date"].astype(int)
        df = df[GROUPBY_COLUMNS + ["personId"]].drop_duplicates()
        df = (
            df.groupby(GROUPBY_COLUMNS, observed=True)["personId"].count().reset_index()
        )
        df = df.rename(columns={"personId": "players"})
        return df
