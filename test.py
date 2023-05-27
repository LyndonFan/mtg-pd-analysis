import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

from dotenv import dotenv_values

conf = {**dotenv_values()}

CREATE_ENUM_QUERY = """
    CREATE TYPE deckSource AS ENUM ('League', 'Gatherling');
"""

CREATE_ARCHETYPE_QUERY = """
    CREATE TABLE IF NOT EXISTS archetypes (
        id INTEGER PRIMARY KEY,
        archetype VARCHAR(64),
    );
"""

CREATE_PEOPLE_QUERY = """
    CREATE TABLE IF NOT EXISTS people (
        id INTEGER PRIMARY KEY,
        name VARCHAR(64),
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
        try:
            with db.cursor() as cur:
                res = func(cur, *args, **kwargs)
            db.commit()
            return res
        except Exception as e:
            raise e
        finally:
            db.close()

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
    query = f"INSERT INTO {tableName} VALUES (%s)"
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
    archetype_df = df[["archetypeId", "archetypeName"]].drop_duplicates()
    people_df = df[["personId", "person"]].drop_duplicates()
    deck_df = df[["id", "name", "seasonId", "sourceName", "personId", "archetypeId"]]
    maindeck_df = df[["id", "maindeck"]].explode("maindeck")
    sideboard_df = df[["id", "sideboard"]].explode("sideboard")
    yb = connect()
    print("Connected to database")
    create_tables(yb)
    print("Created tables")
    insert_into(yb, "archetypes", archetype_df)
    insert_into(yb, "people", people_df)
    insert_into(yb, "decks", deck_df)
    insert_into(yb, "maindecks", maindeck_df)
    insert_into(yb, "sideboards", sideboard_df)


if __name__ == "__main__":
    main()
