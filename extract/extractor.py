from dataclasses import dataclass, field
from typing import Dict, Any, List
import logging

from .paginator import Paginator


@dataclass
class Extractor:
    url: str
    page_size: int = 500
    headers: Dict[str, Any] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    test: bool = False

    def __post_init__(self):
        self.paginator = Paginator(
            url=self.url,
            page_size=self.page_size,
            headers=self.headers,
            params=self.params,
        )

    def execute(self) -> List:
        logging.info(
            f"Start fetching data from {self.url} with {self.page_size=} and paras"
        )
        logging.info(self.params)
        objects = []
        for res in self.paginator.execute():
            objects.extend(res)
            if self.test:
                break
        return objects
