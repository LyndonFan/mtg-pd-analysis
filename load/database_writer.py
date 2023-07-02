import pandas as pd
import numpy as np
import logging
import io
from time import perf_counter

from database import Database
from database.models import Source
from .writer import Writer


class DatabaseWriter(Writer):
    def __init__(self, table_name: str, id_column: str = "id") -> None:
        self.table_name = table_name
        self.id_column = id_column
        self.database = Database()

    @property
    def temp_table_name(self) -> str:
        return f"temp_{self.table_name}"

    def connection(self):
        return self.database.connection()

    @staticmethod
    def fix_columns(columns: list[str]) -> list[str]:
        return [f'"{c}"' for c in columns]

    @staticmethod
    def print_sql(sql: str) -> None:
        logging.info(sql.replace("\t", " ".strip()))

    def _pipe_to_io(self, df: pd.DataFrame) -> io.StringIO:
        s = io.StringIO()
        df.to_csv(s, header=False, index=False, encoding="utf-8")
        s.seek(0)
        return s

    def _generate_copy_sql(self, columns: list[str]) -> str:
        fixed_columns = self.fix_columns(columns)
        sql = f"""
            COPY {self.temp_table_name} 
            ({', '.join(fixed_columns)})
            FROM STDIN WITH CSV;
            """
        return sql

    def _generate_insert_sql(self, columns: list[str], on_conflict_update: bool) -> str:
        fixed_all_columns = self.fix_columns(columns)
        fixed_all_cols_str = ", ".join(fixed_all_columns)
        rest_columns = self.fix_columns([c for c in columns if c != self.id_column])
        insert_sql = f"""
            INSERT INTO {self.table_name}  ({fixed_all_cols_str})
            SELECT {fixed_all_cols_str} FROM {self.temp_table_name}
            ON CONFLICT ("{self.id_column}") """
        if on_conflict_update:
            insert_sql += f"""DO UPDATE
                SET {', '.join(f'{c}=EXCLUDED.{c}' for c in rest_columns)};
            """
        else:
            insert_sql += f"""DO NOTHING;"""
        return insert_sql

    def execute(
        self,
        df: pd.DataFrame,
        *,
        inside_transaction: bool = False,
        on_conflict_update: bool = False,
    ) -> bool:
        print(df.head())
        print(df.dtypes)
        columns = df.columns.tolist()
        assert self.id_column in columns, f"{self.id_column=} not in {columns=}"
        # TODO: handle NAType
        # psycopg2.ProgrammingError: can't adapt type 'NAType'
        # if "sourceName" in df.columns:
        #     df["sourceName"] = df["sourceName"].map(Source.convert)
        # values = (
        #     df.astype("object")
        #     .replace(np.nan, None)
        #     .apply(tuple, axis=1)
        #     .values.tolist()
        # )
        
        # yugabyte db doesn't support ON COMMIT DROP >:(
        create_temp_table_sql = f"""
            CREATE TEMP TABLE {self.temp_table_name}
            (LIKE {self.table_name});
        """
        copy_sql = self._generate_copy_sql(columns)

        insert_sql = self._generate_insert_sql(columns, on_conflict_update)

        drop_temp_table_sql = f"""DROP TABLE {self.temp_table_name};"""

        s = self._pipe_to_io(df)

        def main_logic(cursor):
            # cursor.executemany(insert_sql, values)
            self.print_sql(create_temp_table_sql)
            cursor.execute(create_temp_table_sql)
            self.print_sql(copy_sql)
            s = self._pipe_to_io(df)
            cursor.copy_expert(copy_sql, s)
            self.print_sql(insert_sql)
            cursor.execute(insert_sql)
            self.print_sql(drop_temp_table_sql)
            cursor.execute(drop_temp_table_sql)

        conn = self.database.connection()
        start_time = perf_counter()
        try:
            if inside_transaction:
                with conn.cursor() as cur:
                    main_logic(cur)
            else:
                with conn:
                    with conn.cursor() as cur:
                        main_logic(cur)
                        conn.commit()
            return True
        except Exception as e:
            if inside_transaction:
                conn.rollback()
            logging.error(e.pgerror)
            raise e
        finally:
            end_time = perf_counter()
            logging.info(
                f"Writing {len(df)} rows to {self.table_name} "
                f"took {end_time - start_time:.2f} seconds"
            )