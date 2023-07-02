import pandas as pd
from database.database import Database
from .database_writer import DatabaseWriter


class Loader:
    def __init__(self) -> None:
        pass

    def execute(self, df: pd.DataFrame) -> None:
        df_dict = {}
        df_dict["people"] = df[["personId", "person"]].drop_duplicates()
        df_dict["people"].columns = ["id", "name"]
        df_dict["archetypes"] = df[["archetypeId", "archetypeName"]].drop_duplicates()
        df_dict["archetypes"].columns = ["id", "archetype"]
        df_dict["decks"] = df.drop(
            columns=["maindeck", "sideboard", "person", "archetypeName"]
        )
        df_dict["decks"].to_csv("decks.csv")
        for board in ["maindeck", "sideboard"]:
            cards_df = df[["id", board]].explode(board).reset_index(drop=True)
            cards_df.to_csv(f"{board}_info.csv")
            board_df = pd.DataFrame(cards_df[board].values.tolist())
            board_df.to_csv(f"{board}_cards.csv")
            cards_df = pd.concat([cards_df, board_df], axis=1)
            df_dict[f"{board}s"] = cards_df.drop(columns=board)
            df_dict[f"{board}s"].columns = ["deckId", "n", "name"]
        common_connection = Database.common_connection()
        with common_connection:
            for table_name, _df in df_dict.items():
                DatabaseWriter(table_name).execute(
                    _df,
                    inside_transaction=True,
                    on_conflict_update=table_name != "people",
                )
            common_connection.commit()
