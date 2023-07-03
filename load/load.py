import pandas as pd
from database.database import Database
from .database_writer import DatabaseWriter


class Loader:
    def __init__(self) -> None:
        pass

    def execute(self, df: pd.DataFrame) -> None:
        people_df = df[["personId", "person"]].drop_duplicates()
        people_df.columns = ["id", "name"]
        archetypes_df = df[["archetypeId", "archetypeName"]].drop_duplicates()
        archetypes_df.columns = ["id", "archetype"]
        DatabaseWriter("people").execute(people_df)
        DatabaseWriter("archetypes").execute(archetypes_df)
        decks_df = df.drop(columns=["maindeck", "sideboard", "person", "archetypeName"])
        print(decks_df)
        df_dict = {}
        for board in ["maindeck", "sideboard"]:
            cards_df = df[["id", board]].explode(board).reset_index(drop=True)
            board_df = pd.DataFrame(cards_df[board].values.tolist())
            cards_df = pd.concat([cards_df, board_df], axis=1)
            df_dict[f"{board}s"] = cards_df.drop(columns=board)
            df_dict[f"{board}s"].columns = ["deckId", "n", "name"]
        common_connection = Database.common_connection()
        deck_ids = decks_df["id"].values.tolist()
        decks_df.to_csv("decks.csv", index=False)
        with common_connection:
            # drop_existing_sql = 'DELETE FROM {} WHERE "deckId" IN %(deck_ids)s;'
            # with common_connection.cursor() as cursor:
            #     for table_name in ["maindecks", "sideboards"]:
            #         _sql = drop_existing_sql.format(table_name)
            #         DatabaseWriter.print_sql(_sql)
            #         cursor.execute(_sql, {"deck_ids": tuple(deck_ids)})
            DatabaseWriter("decks").execute(
                decks_df, inside_transaction=True, on_conflict_update=True
            )
            common_connection.commit()
        with common_connection:
            with common_connection.cursor() as cursor:
                res = cursor.execute(
                    "SELECT * FROM decks WHERE id IN %(deck_ids)s;",
                    {"deck_ids": tuple(deck_ids)},
                )
                print(list(res) if res else [])
                if not res:
                    raise ValueError
            common_connection.commit()
        with common_connection:
            for table_name in ["maindecks", "sideboards"]:
                DatabaseWriter(table_name, include_id=False).execute(
                    df_dict[table_name],
                    inside_transaction=True,
                    on_conflict_update=True,
                )
            common_connection.commit()
