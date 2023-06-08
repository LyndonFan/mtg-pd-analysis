import pandas as pd
import psycopg2

from dotenv import dotenv_values
from .writer import Writer

config = {**dotenv_values()}
config["sslMode"] = "verify-full"


class DatabaseWriter(Writer):
    def __init__(self, table_name: str):
        self.table_name = table_name

    def _connect(self) -> psycopg2.connection:
        conn = psycopg2.connect(
            host=config["dbHost"],
            port=config["dbPort"],
            database=config["dbName"],
            user=config["dbUser"],
            password=config["dbPassword"],
            sslmode=config["sslMode"],
            sslrootcert=config["sslRootCert"],
            connect_timeout=10,
        )
        return conn

    def execute(self, df: pd.DataFrame):
        conn = self._connect()
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
