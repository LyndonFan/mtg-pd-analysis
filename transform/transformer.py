import pandas as pd
from typing import List, Callable, Dict, Any
import logging


class Transformer:
    TRANSFORM_NAMES: Dict[str, Callable] = {}

    @classmethod
    def register(cls, name: str):
        def wrapper(func: Callable[..., pd.Series]) -> Callable[..., pd.Series]:
            if name in cls.TRANSFORM_NAMES:
                raise KeyError(f"{name} is already in {cls.TRANSFORM_NAMES}")
            cls.TRANSFORM_NAMES[name] = func
            return func

        return wrapper

    def __init__(
        self,
        transformations: List[Dict[str, Any]],
        schema: Dict[str, Dict[str, str]],
    ):
        self.schema = schema
        self.transformations = []
        MUST_KEYS = ["name", "source", "target"]
        OPT_KEYS = ["args", "kwargs"]
        sources = set(self.schema.keys())
        for dct in transformations:
            if not any(c in dct for c in MUST_KEYS):
                continue
            if dct["name"] in ["execute", "__init__"]:
                continue
            dct["args"] = dct.get("args", [])
            dct["kwargs"] = dct.get("kwargs", {})
            self.transformations.append(dct)
            if isinstance(dct["source"], str):
                sources.add(dct["source"])
            else:
                sources.update(*dct["source"])
        self.source_columns = list(sources)

    def _transform_repr(
        self, function_name: str, target_col: str, source_col: str, args, **kwargs
    ) -> str:
        res_string = f'df["{target_col}"] = {function_name}(df["{source_col}"]'

        def sstr(x) -> str:
            return f'"{x}"' if isinstance(x, str) else str(x)

        if args:
            for a in args:
                res_string += ", " + sstr(a)
        if kwargs:
            for k, v in kwargs.items():
                res_string += f", {k}={sstr(v)}"
        res_string += ")"
        return res_string

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

        return df
