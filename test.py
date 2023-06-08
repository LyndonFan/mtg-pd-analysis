import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from time import perf_counter

from dotenv import dotenv_values

conf = {**dotenv_values()}

CREATE_ENUM_QUERY = """
    DO $$ BEGIN
        CREATE TYPE deckSource AS ENUM ('League', 'Gatherling');
    EXCEPTION
        WHEN duplicate_object THEN null;
    END $$;
"""
CREATE_ARCHETYPE_QUERY = """
    CREATE TABLE IF NOT EXISTS archetypes (
        id INTEGER PRIMARY KEY,
        archetype VARCHAR(64)
    );
"""
CREATE_PEOPLE_QUERY = """
    CREATE TABLE IF NOT EXISTS people (
        id INTEGER PRIMARY KEY,
        name VARCHAR(64)
    );
"""
CREATE_DECKS_QUERY = """
    CREATE TABLE IF NOT EXISTS decks (
        id INTEGER PRIMARY KEY,
        name VARCHAR(128),
        seasonId INTEGER,
        sourceName deckSource,
        personId INTEGER,
        archetypeId INTEGER,
        FOREIGN KEY (personId) REFERENCES people(id),
        FOREIGN KEY (archetypeId) REFERENCES archetypes(id)
    );
"""
CREATE_TABLES_QUERY = """
    CREATE TABLE IF NOT EXISTS {} (
        deckId INTEGER,
        n INTEGER,
        name VARCHAR(128),
        PRIMARY KEY (deckId, n, name),
        FOREIGN KEY (deckId) REFERENCES decks(id)
    );
"""


def connect():
    try:
        yb = psycopg2.connect(
            host=conf["dbHost"],
            port=conf["dbPort"],
            database=conf["dbName"],
            user=conf["dbUser"],
            password=conf["dbPassword"],
            sslmode="verify-full",
            sslrootcert=conf["sslRootCert"],
            connect_timeout=60,
        )
        return yb
    except Exception as e:
        print("Exception while connecting to YugabyteDB")
        raise (e)


def transactional(func):
    def inner(db, *args, **kwargs):
        start_time = perf_counter()
        try:
            with db.cursor() as cur:
                res = func(cur, *args, **kwargs)
            db.commit()
            return res
        except Exception as e:
            db.close()
            raise e
        finally:
            end_time = perf_counter()
            print(f"This took {end_time-start_time:.3f} seconds")

    return inner


@transactional
def create_tables(cur):
    cur.execute(CREATE_ENUM_QUERY)
    cur.execute(CREATE_ARCHETYPE_QUERY)
    cur.execute(CREATE_PEOPLE_QUERY)
    cur.execute(CREATE_DECKS_QUERY)
    for tableName in ["maindecks", "sideboards"]:
        cur.execute(CREATE_TABLES_QUERY.format(tableName))


@transactional
def insert_into(cur, tableName: str, df: pd.DataFrame) -> None:
    query = f"INSERT INTO {tableName} VALUES %s"
    values = df.values.tolist()
    execute_values(cur, query, values)
    print(f"Inserted {len(df)} rows of data into table {tableName}")


def main():
    deck_path = os.path.join(conf["BUCKET"], "decks.parquet")
    COLUMNS = [
        "id",
        "name",
        "seasonId",
        "sourceName",
        "person",
        "personId",
        "archetypeName",
        "archetypeId",
        "maindeck",
        "sideboard",
    ]
    df = pd.read_parquet(deck_path, columns=COLUMNS)
    id_vc = df["id"].value_counts()
    assert ~(id_vc > 1).any(), id_vc[id_vc > 1]
    archetype_df = df[["archetypeId", "archetypeName"]].drop_duplicates()
    people_df = df[["personId", "person"]].drop_duplicates()
    deck_df = df[["id", "name", "seasonId", "sourceName", "personId", "archetypeId"]]
    board_dfs = {}
    for board in ["maindeck", "sideboard"]:
        cards_df = df[["id", board]].explode(board)
        cards_df = cards_df[cards_df[board].notnull()]
        cards_df["n"] = cards_df[board].map(lambda x: x.get("n"))
        cards_df["name"] = cards_df[board].map(lambda x: x.get("name"))
        cards_df = cards_df.drop(columns=[board])
        print(cards_df)
        board_dfs[board] = cards_df
    yb = connect()
    print("Connected to database")
    create_tables(yb)
    print("Created tables")
    # insert_into(yb, "archetypes", archetype_df)
    # insert_into(yb, "people", people_df)
    # insert_into(yb, "decks", deck_df)
    for board, _df in board_dfs.items():
        insert_into(yb, board + "s", _df)
    yb.close()


if __name__ == "__main__":
    main()
