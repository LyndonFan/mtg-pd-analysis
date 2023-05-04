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
        self, function_name: str, target_col: str, source_col: str, *args, **kwargs
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
        KEYS = ["name", "source", "target", "args", "kwargs"]
        source_cols = list(set(self.source_columns) & set(df.columns))
        df = df[source_cols].copy()
        for dct in self.transformations:
            if dct["name"] not in self.TRANSFORM_NAMES:
                logging.warning(
                    f"Function {dct['name']} not found in {self.TRANSFORM_NAMES.keys()=}"
                )
                continue
            name, source, target, args, kwargs = [dct[c] for c in KEYS]
            f = self.TRANSFORM_NAMES[name]
            logging.info(
                "Running " + self._transform_repr(name, target, source, *args, **kwargs)
            )
            df[target] = f(df[source], *args, **kwargs)
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
