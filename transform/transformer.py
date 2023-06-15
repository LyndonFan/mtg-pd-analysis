import pandas as pd
from typing import List, Callable, Dict, Any
import logging


class Transformer:
    def __init__(
        self,
        schema: Dict[str, Dict[str, str]],
    ):
        self.schema = schema
        sources = set(self.schema.keys())
        self.source_columns = list(sources)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df["url"] = "https://pennydreadfulmagic.com" + df["url"]
        df["omw"] = df["omw"].str.replace("%", "")
        df["omwPercent"] = df["omw"].replace("", None)
        df["matches"] = df[["wins", "losses", "draws"]].sum(axis=1)
        df["archetypeId"] = df["archetypeId"].fillna(-1)
        df["archetypeName"] = df["archetypeName"].fillna("N/A")
        df["archetypeName"] = df["archetypeName"].replace({})
        for c in "WUBRGC":
            df[f"colorHas{c}"] = df["colors"].apply(lambda x: c in x)
        df["createdDatetime"] = pd.to_datetime(df["createdDate"], unit="s")
        df["createdDate"] = df["createdDatetime"].dt.strftime("%Y%m%d")
        df["updatedDatetime"] = pd.to_datetime(df["updatedDate"], unit="s")
        df["updatedDate"] = df["updatedDatetime"].dt.strftime("%Y%m%d")

        keep_columns = []
        for source, dct in self.schema.items():
            if dct["dtype"] == "object":
                logging.info(f"Setting {source} as dtype=object")
                keep_columns.append(source)
                continue
            else:
                logging.info(f"Setting {source} as dtype={dct['dtype']}")
                df.loc[df[source].isna(), source] = None
                df[source] = df[source].astype(dct["dtype"])
            if dct["dtype"] == "category" and "categories" in dct:
                logging.info(f"Setting categories for {source} as {dct['categories']}")
                df[source] = df[source].cat.set_categories(dct["categories"])
            keep_columns.append(source)
        df = df[keep_columns].copy()
        return df
