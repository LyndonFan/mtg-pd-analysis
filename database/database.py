import pandas as pd
import psycopg2
from typing import Any

from dotenv import dotenv_values


conf = {**dotenv_values()}
conf["sslMode"] = "verify-full"


class Database:
    def __init__(self, config: "dict[str, Any] | None" = None):
        if config is None:
            config = conf
        self.config = config


    def _connect(self) -> psycopg2.connection:
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
        return conn
    
    