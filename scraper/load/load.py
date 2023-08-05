import pandas as pd
from database.database import Database
from .database_writer import DatabaseWriter
import logging


class Loader:
    def __init__(self) -> None:
        pass

    def execute(self, df: pd.DataFrame) -> None:
        print(f"Originally have {len(df)} rows")
        df = self.pre_filter(df)
        print(f"Filtered to {len(df)} rows")

        people_df = df[["personId", "person"]].drop_duplicates()
        people_df.columns = ["id", "name"]
        archetypes_df = df[["archetypeId", "archetypeName"]].drop_duplicates()
        archetypes_df.columns = ["id", "archetype"]
        DatabaseWriter("people").execute(people_df)
        DatabaseWriter("archetypes").execute(archetypes_df)

        decks_df = df.drop(columns=["maindeck", "sideboard", "person", "archetypeName"])
        decks_df.to_csv("decks.csv", index=False)

        df_dict = {}
        for board in ["maindeck", "sideboard"]:
            cards_df = df[["id", board]].explode(board)
            # [] explodes to nan, so have to drop them
            cards_df = cards_df.dropna(subset=[board]).reset_index(drop=True)
            board_df = pd.DataFrame(cards_df[board].values.tolist())
            cards_df = pd.concat([cards_df, board_df], axis=1)
            df_dict[f"{board}s"] = cards_df.drop(columns=board)
            df_dict[f"{board}s"].columns = ["deckId", "n", "name"]

        common_connection = Database.common_connection()
        with common_connection:
            with common_connection.cursor() as cursor:
                cursor.execute(
                    """
                    DROP TABLE IF EXISTS temp_deck_ids;
                    CREATE TABLE temp_deck_ids (id INTEGER PRIMARY KEY);
                    """
                )
                common_connection.commit()
            DatabaseWriter("temp_deck_ids").execute(
                decks_df[["id"]].drop_duplicates(),
                inside_transaction=True,
                on_conflict="error",
            )
            with common_connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT count(1) FROM decks
                    WHERE id IN (SELECT id FROM temp_deck_ids);
                    """
                )
                res = cursor.fetchone()
            if res is None:
                raise ValueError(f"Failed to load decks")
            num_rows = res[0]
            print(f"Need to update {num_rows} decks")
            if num_rows:
                # TODO
                # delete from decks instead and cascade to tables
                for table_name in ["maindecks", "sideboards"]:
                    delete_sql = f"""
                        DELETE FROM {table_name} WHERE "deckId" IN
                        (SELECT id FROM temp_deck_ids);
                        """
                    logging.info(delete_sql)
                    with common_connection.cursor() as cursor:
                        cursor.execute(delete_sql)
                common_connection.commit()
            DatabaseWriter("decks").execute(
                decks_df, inside_transaction=True, on_conflict="update"
            )
            for table_name in ["maindecks", "sideboards"]:
                DatabaseWriter(table_name, include_id=False).execute(
                    df_dict[table_name],
                    inside_transaction=True,
                    on_conflict="error",
                )
            common_connection.commit()
            with common_connection.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS temp_deck_ids;")
            common_connection.commit()

    def pre_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        seasonId = int(df["seasonId"].min())
        with Database.common_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                SELECT id, "updatedDatetime"
                from decks
                where "seasonId" = %s
                """
                cur.execute(sql, (seasonId,))
                res = cur.fetchall()
        last_updated_df = pd.DataFrame(res, columns=["id", "lastUpdated"])
        df = df.merge(last_updated_df, on="id", how="left")
        df = df[
            (df["lastUpdated"].isna()) | (df["lastUpdated"] < df["updatedDatetime"])
        ]
        df = df.drop(columns=["lastUpdated"])
        return df
