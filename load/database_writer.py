import pandas as pd
import logging

from dotenv import dotenv_values
from database import Database
from .writer import Writer


class DatabaseWriter(Writer):
    def __init__(self, table_name):
        self.table_name = table_name
        self.database = Database()

    def connection(self):
        return self.database.connection()

    def execute(self, df: pd.DataFrame, inside_transaction: bool = False) -> bool:
        columns = list(df.columns)
        values = [tuple(row) for row in df.values]
        placeholders = ",".join(["%s"] * len(columns))
        insert_sql = f"""
            INSERT INTO {self.table_name} ({','.join(columns)}) VALUES ({placeholders})
            ON CONFLICT DO UPDATE
        """
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