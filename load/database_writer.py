import pandas as pd
import psycopg2

from dotenv import dotenv_values
from .writer import Writer
from .database import Database


class DatabaseWriter(Writer):
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.database = Database()


    def execute(self, df: pd.DataFrame):
        conn = self.database.connection()
        columns = list(df.columns)
        values = [tuple(row) for row in df.values]
        placeholders = ",".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {self.table_name} ({','.join(columns)}) VALUES ({placeholders})"
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.executemany(insert_sql, values)
        except Exception as e:
            raise e
        finally:
            conn.close()
