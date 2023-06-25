import logging
import psycopg2

from dotenv import dotenv_values


conf = {**dotenv_values()}


class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def common_connection(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance.connection()

    def __init__(self):
        self.config = conf
        self._connection = None

    def connection(self) -> "psycopg2.connection":
        if self._connection is None:
            self._connect()
        elif self._connection.closed:
            self._connect()
        return self._connection

    def _connect(self) -> None:
        try:
            conn = psycopg2.connect(
                host=self.config["dbHost"],
                port=self.config["dbPort"],
                database=self.config["dbName"],
                user=self.config["dbUser"],
                password=self.config["dbPassword"],
                sslmode="verify-full",
                sslrootcert=self.config["sslRootCert"],
                connect_timeout=10,
            )
        except Exception as e:
            logging.error(e)
            logging.info("Trying ssl mode = 'require")
            conn = psycopg2.connect(
                host=self.config["dbHost"],
                port=self.config["dbPort"],
                database=self.config["dbName"],
                user=self.config["dbUser"],
                password=self.config["dbPassword"],
                sslmode="require",
                connect_timeout=10,
            )
        self._connection = conn
