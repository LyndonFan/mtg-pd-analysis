import pandas as pd
import logging

from dotenv import dotenv_values
from database import Database
from .writer import Writer


class DatabaseWriter(Writer):
    def __init__(self, table_name: str, id_column: str = "id") -> None:
        self.table_name = table_name
        self.id_column = id_column
        self.database = Database()

    def connection(self):
        return self.database.connection()

    def execute(
        self,
        df: pd.DataFrame,
        inside_transaction: bool = False,
        on_conflict_update: bool = False,
    ) -> bool:
        columns = df.columns.tolist()
        assert self.id_column in columns, f"{self.id_column=} not in {columns=}"
        # TODO: handle NAType
        # psycopg2.ProgrammingError: can't adapt type 'NAType'
        values = [tuple(row) for row in df.values]
        placeholders = ",".join(["%s"] * len(columns))
        rest_columns = [c for c in columns if c != self.id_column]
        insert_sql = f"""
            INSERT INTO {self.table_name} ({','.join(columns)}) VALUES ({placeholders})
            ON CONFLICT ({self.id_column}) """
        if on_conflict_update:
            insert_sql += f"""DO UPDATE
                SET {', '.join(f'{c}=EXCLUDED.{c}' for c in rest_columns)}
            """
        else:
            insert_sql += f"""DO NOTHING"""
        logging.info(insert_sql)
        conn = self.database.connection()
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
            if inside_transaction:
                conn.rollback()
            conn.close()
