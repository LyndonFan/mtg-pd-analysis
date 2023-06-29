import pandas as pd
import numpy as np
import logging
from time import perf_counter

from dotenv import dotenv_values
from database import Database
from database.models import Source
from .writer import Writer


class DatabaseWriter(Writer):
    def __init__(self, table_name: str, id_column: str = "id") -> None:
        self.table_name = table_name
        self.id_column = id_column
        self.database = Database()

    def connection(self):
        return self.database.connection()
    
    @staticmethod
    def fix_columns(columns: list[str]) -> list[str]:
        return [f'"{c}"' for c in columns]

    def _generate_sql(self, columns: list[str], on_conflict_update: bool) -> str:
        placeholders = ",".join(["%s"] * len(columns))
        rest_columns = self.fix_columns([c for c in columns if c != self.id_column])
        insert_sql = f"""
            INSERT INTO {self.table_name} ({','.join(self.fix_columns(columns))}) VALUES ({placeholders})
            ON CONFLICT ({self.id_column}) """
        if on_conflict_update:
            insert_sql += f"""DO UPDATE
                SET {', '.join(f'{c}=EXCLUDED.{c}' for c in rest_columns)}
            """
        else:
            insert_sql += f"""DO NOTHING"""
        return insert_sql

    def execute(
        self,
        df: pd.DataFrame,
        *,
        inside_transaction: bool = False,
        on_conflict_update: bool = False,
    ) -> bool:
        columns = df.columns.tolist()
        assert self.id_column in columns, f"{self.id_column=} not in {columns=}"
        # TODO: handle NAType
        # psycopg2.ProgrammingError: can't adapt type 'NAType'
        if "sourceName" in df.columns:
            df["sourceName"] = df["sourceName"].map(Source.convert)
        values = (
            df.astype("object")
            .replace(np.nan, None)
            .apply(tuple, axis=1)
            .values.tolist()
        )
        insert_sql = self._generate_sql(columns, on_conflict_update)
        logging.info(insert_sql)
        conn = self.database.connection()
        start_time = perf_counter()
        try:
            if inside_transaction:
                with conn.cursor() as cur:
                    cur.executemany(insert_sql, values)
            else:
                with conn:
                    with conn.cursor() as cur:
                        cur.executemany(insert_sql, values)
            return True
        except Exception as e:
            raise e
        finally:
            end_time = perf_counter()
            logging.info(
                f"Writing {len(values)} rows to {self.table_name} took {end_time - start_time:.2f} seconds"
            )
            if inside_transaction:
                conn.rollback()
            conn.close()
