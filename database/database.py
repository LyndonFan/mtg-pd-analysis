import pandas as pd
import psycopg2
from typing import Any

from dotenv import dotenv_values


conf = {**dotenv_values()}
conf["sslMode"] = "verify-full"


class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.config = conf
        self._connection = None

    def connection(self) -> "psycopg2.connection":
        if self._connection is None:
            self._connect()
        return self._connection

    def _connect(self) -> None:
        conn = psycopg2.connect(
            host=self.config["dbHost"],
            port=self.config["dbPort"],
            database=self.config["dbName"],
            user=self.config["dbUser"],
            password=self.config["dbPassword"],
            sslmode=self.config["sslMode"],
            sslrootcert=self.config["sslRootCert"],
            connect_timeout=10,
        )
        self._connection = conn
