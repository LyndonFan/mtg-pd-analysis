import pandas as pd
from abc import ABC


class Writer(ABC):
    def execute(self, df: pd.DataFrame, *args, **kwargs):
        """
        Writes to the given location with the given pandas DataFrame as input.

        Args:
            df (pd.DataFrame): The input DataFrame for the function.

        Returns:
            None
        """
        raise NotImplementedError
