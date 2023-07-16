import json
from dataclasses import dataclass, field
import pandas as pd
from datetime import datetime

import logging
from database import Database
from .extractor import Extractor

SEASON_URL = "https://pennydreadfulmagic.com/api/seasoncodes"
with open("headers.json") as f:
    HEADERS = json.load(f)

"""
since /updated endpoint doesn't work for some reason

only useful option for sortBy is "date",
and we can do "sortOrder=DESC" as well
https://github.com/PennyDreadfulMTG/Penny-Dreadful-Tools/blob/ac5746779e1b139edff37f349a75e4f7b4f8a677/decksite/data/query.py#L75

"""


@dataclass
class Preparer:
    seasonId: "int | None"
    lastUpdated: datetime = field(
        init=False, default_factory=lambda: datetime.fromtimestamp(0)
    )

    @property
    def since(self) -> int:
        return int(self.lastUpdated.timestamp())

    def __post_init__(self):
        self.db = Database()
    
    def execute(self):
        if self.seasonId is None:
            self.seasonId = self.infer_season_id()
        self.lastUpdated = self.get_last_updated()

    def infer_season_id(self) -> int:
        logging.info(f"seasonId not provided, checking for total number of seasons...")
        extractor = Extractor(url=SEASON_URL, headers=HEADERS)
        season_codes = extractor.execute()
        return len(season_codes)


    def get_last_updated(self) -> datetime:
        # first season was Eldritch moon, released on July 22, 2016
        query = """
            SELECT
            IFNULL(max("updatedDatetime"), timestamp '2016-01-01')
            FROM decks
            WHERE "seasonId" = %s;
        """
        with self.db.common_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (self.seasonId,))
                res = cur.fetchone()
        if res is None:
            logging.warn(f"Unable to find any decks for season {self.seasonId}")
            logging.warn(f"Will use 0 to search all decks.")
            return datetime.fromtimestamp(0)
        return res[0]
