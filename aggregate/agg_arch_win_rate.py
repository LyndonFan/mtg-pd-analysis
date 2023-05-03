import pandas as pd
from typing import List
import logging

from .aggregator import Aggregator
from .aggregate_manager import AggregateManager


@AggregateManager.register("archetype_win_rate")
class ArchetypeWinRateAggregator(Aggregator):
    def __init__(self) -> None:
        groupby_cols = [
            "seasonId",
            "sourceName",
            "archetypeId",
            "archetypeName",
        ]
        extra_columns = ["personId", "wins", "matches"]
        super().__init__(groupby_cols, groupby_cols + extra_columns)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._preprocess(df)
        df = df[df["matches"] > 2].copy()
        df = df.groupby(self.groupby_columns, observed=True, as_index=False).agg(
            players=pd.NamedAgg("personId", "nunique"),
            decks=pd.NamedAgg("wins", "count"),
            wins=pd.NamedAgg("wins", "sum"),
            matches=pd.NamedAgg("matches", "sum"),
        )
        df["winRate"] = df["wins"] * 1.0 / df["matches"]
        df["smoothedWinRate"] = (df["wins"] + 1) / (df["matches"] + 2.0)
        return df
