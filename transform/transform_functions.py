import pandas as pd
import json
from typing import Any

from .transformer import Transformer


@Transformer.register("isin_list")
def isin_list(ss: pd.Series, value: Any) -> pd.Series:
    return pd.Series(value in v for v in ss)


@Transformer.register("str_replace")
def str_replace(ss: pd.Series, *args, **kwargs) -> pd.Series:
    return ss.str.replace(*args, **kwargs)


@Transformer.register("replace")
def replace(ss: pd.Series, *args, **kwargs) -> pd.Series:
    return ss.replace(*args, **kwargs)


@Transformer.register("fillna")
def fillna(ss: pd.Series, *args, **kwargs) -> pd.Series:
    return ss.fillna(*args, **kwargs)


@Transformer.register("prepend")
def prepend(ss: pd.Series, value: str) -> pd.Series:
    return value + ss


@Transformer.register("to_datetime")
def to_datetime(ss: pd.Series, *args, **kwargs) -> pd.Series:
    return pd.to_datetime(ss, *args, **kwargs)


@Transformer.register("strftime")
def strftime(ss: pd.Series, date_format: str) -> pd.Series:
    return ss.dt.strftime(date_format)


@Transformer.register("column_sum")
def column_sum(df: pd.DataFrame) -> pd.Series:
    return df.sum(axis=1)
