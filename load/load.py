import pandas as pd
from database.database import Database
from .database_writer import DatabaseWriter


class Loader:
    def __init__(self) -> None:
        pass

    def execute(self, df: pd.DataFrame) -> None:
        people_df = df[["personId", "person"]].drop_duplicates()
        people_df.columns = ["id", "name"]
        DatabaseWriter("people").execute(people_df)
        archetype_df = df[["archetypeId", "archetypeName"]].drop_duplicates()
        archetype_df.columns = ["id", "archetype"]
        DatabaseWriter("archetypes").execute(archetype_df, on_conflict_update=True)
        common_connection = Database.common_connection()
        info_df = df.drop(columns=["maindeck", "sideboard", "person", "archetypeName"])
        with common_connection:
            DatabaseWriter("decks").execute(info_df, True, True)
            for board in ["maindeck", "sideboard"]:
                cards_df = df[["id", board]].explode(board)
                board_df = pd.DataFrame(cards_df[board].values.tolist())
                cards_df = pd.concat([cards_df, board_df], axis=1)
                cards_df = cards_df.drop(columns=board)
                cards_df.columns = ["deckId", "n", "name"]
                DatabaseWriter(board + "s", "deckId").execute(cards_df, True, True)
