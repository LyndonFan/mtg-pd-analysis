import re
import pandas as pd
import logging
import io
from time import perf_counter

from database import Database
from .writer import Writer


class DatabaseWriter(Writer):
    def __init__(self, table_name: str, include_id: bool = True) -> None:
        self.table_name = table_name
        self.include_id = include_id
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
        lines = sql.strip().split("\n")
        logging.info("\n" + "\n".join("\t" + l.strip() for l in lines))

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
        insert_sql = f"""
            INSERT INTO {self.table_name}  ({fixed_all_cols_str})
            SELECT {fixed_all_cols_str} FROM {self.temp_table_name}
            ON CONFLICT ("id") """
        if on_conflict_update:
            rest_columns = self.fix_columns([c for c in columns if c != "id"])
            lhs = ", ".join(rest_columns)
            rhs = ", ".join(f"EXCLUDED.{c}" for c in rest_columns)
            insert_sql += f"DO UPDATE SET ({lhs}) = ({rhs});"
        else:
            insert_sql += "DO NOTHING;"
        # insert_sql += "\nRETURNING id;"
        return insert_sql

    def execute(
        self,
        df: pd.DataFrame,
        *,
        inside_transaction: bool = False,
        on_conflict_update: bool = False,
    ) -> bool:
        logging.info(df.shape)
        columns = df.columns.tolist()
        if self.include_id:
            assert "id" in columns, f'"id" not in {columns=}'

        # yugabyte db doesn't support ON COMMIT DROP >:(
        create_temp_table_sql = f"""
            DROP TABLE IF EXISTS {self.temp_table_name};
            CREATE TEMP TABLE {self.temp_table_name}
            (LIKE {self.table_name});
        """
        copy_sql = self._generate_copy_sql(columns)

        insert_sql = self._generate_insert_sql(columns, on_conflict_update)

        def main_logic(cursor):
            # default executemany is just running loop under the hood
            # so very slow
            self.print_sql(create_temp_table_sql)
            cursor.execute(create_temp_table_sql)
            self.print_sql(copy_sql)
            s = self._pipe_to_io(df)
            cursor.copy_expert(copy_sql, s)
            check_sql = f"""SELECT count(1) FROM {self.temp_table_name};"""
            self.print_sql(check_sql)
            cursor.execute(check_sql)
            num_copied_rows = cursor.fetchone()[0]
            if num_copied_rows != len(df):
                raise ValueError(f"{num_copied_rows=} != {len(df)=}")
            self.print_sql(insert_sql)
            cursor.execute(insert_sql)

        conn = self.database.connection()
        start_time = perf_counter()
        try:
            if inside_transaction:
                with conn.cursor() as cur:
                    main_logic(cur)
                    conn.commit()
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
