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

@dataclass
class Preparer:
    seasonId: "int | None"
    lastUpdated: datetime = field(
        init=False, default_factory=lambda: datetime.fromtimestamp(0)
    )

    def __post_init__(self):
        self.db = Database()
    
    def execute(self):
        if self.seasonId is None:
            self.seasonId = self.get_actual_season()
        self.lastUpdated = self.get_last_updated()

    def get_actual_season(self) -> int:
        logging.info(f"seasonId not provided, checking for total number of seasons...")
        extractor = Extractor(url=SEASON_URL, headers=HEADERS)
        season_codes = extractor.execute()
        return len(season_codes)


    def get_last_updated(self) -> datetime:
        query = """
            SELECT max("updatedDatetime") FROM decks
            WHERE "seasonId" = %s;
        """
        with self.db.common_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (self.seasonId,))
                res = cur.fetchone()
        if res is None:
            raise ValueError(f"Failed to load decks")
        return res[0]
