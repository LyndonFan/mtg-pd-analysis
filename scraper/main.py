import json
import base64
import os
import pandas as pd
import logging

from extract import Extractor, Preparer
from transform import Transformer
from load import Loader
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
TARGET = os.environ["TARGET"]
BUCKET = os.environ["BUCKET"]
PARTITION_COLS = ["seasonId", "archetypeId", "archetypeName"]

# notifier = Notifier(os.environ["EMAIL"])


@error_wrapper
def main(seasonId: "int | None" = None, test: bool = False):
    if test:
        logging.getLogger().setLevel(logging.DEBUG)
    prep = Preparer(seasonId=seasonId)
    prep.execute()
    seasonId = prep.seasonId
    sinceAsString = prep.lastUpdated.strftime("%Y%m%d %H:%M:%S")
    logging.info(f"Running with {seasonId=}, since={sinceAsString}, {test=}")
    params = {"seasonId": seasonId, "since": prep.since}
    extractor = Extractor(
        url=URL,
        headers=HEADERS,
        params=params,
        page_size=(10 if test else 500),
        test=test,
    )
    objects = extractor.execute()
    df = pd.DataFrame(objects)
    logging.info("Extractor done")

    transformer = Transformer(schema=SCHEMA)
    df = transformer.execute(df)
    logging.info("Transformer done")
    logging.info(f"{df.shape=}")

    loader = Loader()
    loader.execute(df)
    logging.info("Loader done")

    # writer = ParquetWriter(
    #     target=TARGET,
    #     bucket=BUCKET,
    # )
    # writer.execute(
    #     df,
    #     filename="decks.parquet",
    #     partition_cols=PARTITION_COLS,
    # )
    # logging.info("Writer done")

    # agg_manager = AggregateManager(writer)
    # agg_manager.execute(df)
    # logging.info("Aggregations done")
    # logging.info("All done")
    return


def entry_point(event, context):
    try:
        pubsub_message = event["data"]
    except KeyError:
        return {"status": "fail", "reason": "unable to find data"}, 500
    try:
        message_data = base64.b64decode(pubsub_message).decode("utf-8")
    except ValueError:
        return {"status": "fail", "reason": "unable to decode data from base64"}, 500
    try:
        message_dict = json.loads(message_data)
    except json.decoder.JSONDecodeError:
        return {"status": "fail", "reason": "unable to decode data as json"}, 500
    seasonId = message_dict.get("seasonId")
    test = "test" in message_dict and message_dict["test"] is True
    main(seasonId, test)


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
    # optional argument called --test
    p.add_argument(
        "--test",
        dest="test",
        action="store_true",
        default=False,
        help="Run in test mode",
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
