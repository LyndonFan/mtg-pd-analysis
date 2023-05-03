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

URL = "https://pennydreadfulmagic.com/api/decks/"
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
def main(test=False):
    params = {"seasonId": 28}
    extractor = Extractor(url=URL, headers=HEADERS, params=params, test=test)
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
    logging.basicConfig(level=logging.DEBUG)
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--test", dest="test", action="store_true", default=False)
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
