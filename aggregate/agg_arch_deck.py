from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Union, Callable
import logging

from .aggregator import Aggregator
from .aggregate_manager import AggregateManager

GROUPBY_COLUMNS = [
    "seasonId",
    "sourceName",
    "archetypeId",
    "archetypeName"
]

class Strategy(ABC):
    def __init__(self, columns: List[str]) -> None:
        self.columns = columns
    
    @abstractmethod
    def weight_function(self, df: pd.DataFrame) -> pd.Series:
        raise NotImplementedError


class ArchetypeDeckAggregator(Aggregator):
    def __init__(
        self,
        strategy: Strategy
    ) -> None:
        self.strategy = strategy
        extra_columns = strategy.columns
        if not extra_columns:
            extra_columns = ["maindeck", "sideboard"]
        for c in ["maindeck", "sideboard"]:
            if c not in extra_columns:
                extra_columns.append(c)
        super().__init__(
            GROUPBY_COLUMNS + extra_columns
        )
    
    @staticmethod
    def expand_cards(deck: "list[dict[str, str|int]]") -> "list[str]":
        return [f"{r['name']} {i:02}" for r in deck for i in range(r['n'])]
    
    @staticmethod
    def aggregate_cards(df: pd.DataFrame, total: int) -> pd.DataFrame:
        """
        Given a dataframe of weights and cards,
        returns a list of `total` cards with highest weights
        for each groupby combination

        Args:
            df (pd.DataFrame): dataframe with GROUPBY_COLUMNS, "weight", and "card"
            total (int): number of cards to take
        
        Returns:
            pd.DataFrame: dataframe with GROUPBY_COLUMNS, and "card",
            which contains `total` cards of the highest weights
        """
        values = []
        # use loop instead since sort_values over entire df is very slow
        for gp, _df in df.groupby(GROUPBY_COLUMNS):
            _df = _df.groupby("card")["weight"].sum().reset_index()
            _df["card"] = _df["card"].str.replace(" [0-9]+$","",regex=True)
            _df = _df.sort_values(by="weight", ascending=False)
            cards_vc = _df["card"][:total].value_counts().reset_index()
            cards_ls = cards_vc.rename(columns={'index':'name',0:'n'}).to_dict(orient='records')
            values.append([*gp, cards_ls])
        res_df = pd.DataFrame(values, columns=GROUPBY_COLUMNS+["card"])
        return res_df
        

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._preprocess(df)
        counts = df.groupby(GROUPBY_COLUMNS).size().reset_index()
        counts = counts.rename({0: "decks"}, axis=1)
        df["weight"] = self.strategy.weight_function(df)
        dfs = []
        for col, n in [("maindeck", 60), ("sideboard", 15)]:
            df[col] = df[col].map(self.expand_cards)
            temp_df = df[GROUPBY_COLUMNS + [col, "weight"]].rename(columns={col: "card"})
            temp_df = temp_df.explode("card")
            temp_df = self.aggregate_cards(temp_df, n)
            dfs.append(temp_df.rename(columns={"card": col}))
        df = dfs[0].merge(dfs[1], on=GROUPBY_COLUMNS, how="outer")
        df = df.merge(counts, on=GROUPBY_COLUMNS, how="outer")
        df = df[GROUPBY_COLUMNS + ["decks", "maindeck", "sideboard"]]
        return df

class AverageStrategy(Strategy):
    def __init__(self) -> None:
        super().__init__(["matches"])

    def weight_function(self, df: pd.DataFrame) -> pd.Series:
        return df["matches"]

@AggregateManager.register("average_archetype_deck")
class AverageArchetypeDeckAggregator(ArchetypeDeckAggregator):
    def __init__(self) -> None:
        super().__init__(AverageStrategy())

class NetWinsStrategy(Strategy):
    def __init__(self) -> None:
        super().__init__(["wins", "losses"])

    def weight_function(self, df: pd.DataFrame) -> pd.Series:
        return (df["wins"] - df["losses"]).clip(lower=0)

@AggregateManager.register("average_net_wins_archetype_deck")
class NetWinsArchetypeDeckAggregator(ArchetypeDeckAggregator):
    def __init__(self) -> None:
        super().__init__(NetWinsStrategy())