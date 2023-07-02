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
        decks_df = df.drop(
            columns=["maindeck", "sideboard", "person", "archetypeName"]
        )
        df_dict = {}
        for board in ["maindeck", "sideboard"]:
            cards_df = df[["id", board]].explode(board).reset_index(drop=True)
            board_df = pd.DataFrame(cards_df[board].values.tolist())
            cards_df = pd.concat([cards_df, board_df], axis=1)
            df_dict[f"{board}s"] = cards_df.drop(columns=board)
            df_dict[f"{board}s"].columns = ["deckId", "n", "name"]
        common_connection = Database.common_connection()
        with common_connection:
            DatabaseWriter("decks").execute(decks_df, inside_transaction=True)
            # TODO: handle deck id update properly
            for table_name in ["maindecks", "sideboards"]:
                DatabaseWriter(table_name, include_id=False).execute(
                    df_dict[table_name],
                    inside_transaction=True,
                    on_conflict_update=True,
                )
            common_connection.commit()
