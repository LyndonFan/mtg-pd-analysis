from dataclasses import dataclass, field
from typing import Dict, Any
import pandas as pd
import logging

from .paginator import get_paginate_response


@dataclass
class Extractor:
    url: str
    page_size: int = 500
    headers: Dict[str, Any] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    test: bool = False

    def execute(self) -> pd.DataFrame:
        logging.info(
            f"Start fetching data from {self.url} with {self.page_size=} and paras"
        )
        logging.info(self.params)
        objects = []
        for res in get_paginate_response(
            self.url,
            page_size=self.page_size,
            headers=self.headers,
            params=self.params,
        ):
            objects.extend(res)
            if self.test:
                break
        df = pd.DataFrame(objects)
        return df
