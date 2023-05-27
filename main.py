import json
import os
import pandas as pd
import logging
from typing import Literal

from extract import Extractor
from transform import Transformer
from load import Writer
from aggregate import AggregateManager
from handler import error_wrapper

# from notifier import Notifier

from dotenv import load_dotenv

load_dotenv()

URL = "https://pennydreadfulmagic.com/api/decks"
with open("headers.json") as f:
    HEADERS = json.load(f)
with open("transform/schema.json") as f:
    SCHEMA = json.load(f)
with open("transform/transformations.json") as f:
    TRANSFORMATIONS = json.load(f)
TARGET = os.environ["TARGET"]
BUCKET = os.environ["BUCKET"]
PARTITION_COLS = ["seasonId", "archetypeId", "archetypeName"]

# notifier = Notifier(os.environ["EMAIL"])


@error_wrapper
def main(seasonId: "int | None" = None):
    if seasonId is None:
        logging.info(f"seasonId not provided, checking for total number of seasons...")
        season_codes_url = URL.replace("decks", "seasoncodes")
        extractor = Extractor(url=season_codes_url, headers=HEADERS)
        season_codes = extractor.execute()
        seasonId = len(season_codes)
    logging.info(f"Running with {seasonId=}")
    params = {"seasonId": seasonId}
    extractor = Extractor(url=URL, headers=HEADERS, params=params)
    objects = extractor.execute()
    df = pd.DataFrame(objects)
    logging.info("Extractor done")
    # df.to_csv("df.csv", index=False)

    transformer = Transformer(
        transformations=TRANSFORMATIONS,
        schema=SCHEMA,
    )
    df = transformer.execute(df)
    logging.info("Transformer done")
    logging.info(f"{df.shape=}")
    writer = Writer(
        target=TARGET,
        bucket=BUCKET,
    )
    writer.execute(
        df,
        filename="decks.parquet",
        partition_cols=PARTITION_COLS,
    )
    logging.info("Writer done")
    agg_manager = AggregateManager(writer)
    agg_manager.execute(df)
    logging.info("Aggregations done")
    logging.info("All done")
    return


def entry_point(event, context):
    main()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument(
        metavar="SEASONID",
        dest="seasonId",
        nargs="?",
        type=int,
        help="Season ID to fetch and process. If not provided, uses latest season",
    )
    import time

    args = p.parse_args()
    s = time.perf_counter()
    try:
        main(**vars(args))
    except Exception as e:
        logging.error(e)
    e = time.perf_counter()
    from datetime import timedelta

    taken = timedelta(seconds=e - s)
    logging.info(f"Time taken: {taken}")
    exit(0)
