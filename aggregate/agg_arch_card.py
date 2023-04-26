import pandas as pd
import logging

from .aggregator import Aggregator
from .aggregate_manager import AggregateManager

GROUPBY_COLUMNS = [
    "seasonId",
    "sourceName",
    "archetypeId",
    "archetypeName"
]

class ArchetypeCardsAggregator(Aggregator):
    def __init__(self, column: str) -> None:
        self.column = column
        extra_columns = ["matches", "wins"]
        extra_columns.append(column)
        super().__init__(
            GROUPBY_COLUMNS + extra_columns
        )

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._preprocess(df)
        df = df.rename(columns={self.column: "card"})
        res_dfs = []
        for grp, _df in df.groupby(GROUPBY_COLUMNS, observed=True):
            n_decks = len(_df)
            _df = _df.explode("card").dropna(subset="card")
            if _df.empty:
                logging.warn(f"No cards found for {grp}")
                continue
            card_info = pd.DataFrame(_df["card"].tolist())
            _df = _df.drop(columns=["card"]).reset_index(drop=True)
            _df = pd.concat([_df, card_info], axis=1).rename(columns={"name": "card"})
            _df = _df.groupby(["card"]).agg(
                decks=pd.NamedAgg("wins", "count"),
                n=pd.NamedAgg("n", "sum"),
                wins=pd.NamedAgg("wins", "sum"),
                matches=pd.NamedAgg("matches", "sum")
            )
            _df["includeRate"] = _df["decks"] * 1.0 / n_decks
            _df["includeAverageNumber"] = _df["n"] * 1.0 / _df["decks"]
            _df["winRate"] = _df["wins"] * 1.0 / _df["matches"]
            _df = _df.drop(columns=["n"])
            for c, v in zip(GROUPBY_COLUMNS, grp):
                _df[c] = v
            res_dfs.append(_df)
        df = pd.concat(res_dfs, axis=0, ignore_index=True)
        return df

@AggregateManager.register("average_archetype_maindeck")
class ArchetypeMaindeckAggregator(ArchetypeCardsAggregator):
    def __init__(self) -> None:
        super().__init__("maindeck")

@AggregateManager.register("average_archetype_sideboard")
class ArchetypeSideboardAggregator(ArchetypeCardsAggregator):
    def __init__(self) -> None:
        super().__init__("sideboard")