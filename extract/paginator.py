import requests
from dataclasses import dataclass, field
from math import ceil, log10
from typing import Any, Dict, List, Generator
import time

import logging

WAIT_RESPONSE_CODES: List[int] = [429, 503, 504]


@dataclass
class Paginator:
    url: str
    page_size: int = 500
    min_time_between_calls: float = 0.5
    headers: Dict[str, Any] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if "page" in self.params:
            self.params.pop("page")
        if "pageSize" not in self.params:
            self.params["pageSize"] = self.page_size

    def get_or_retry(
        self,
        session: requests.Session,
        params: Dict[str, Any],
        max_num_retries: int = 7,
        backoff_factor: float = 2,
    ) -> requests.Response:
        assert max_num_retries > 0, f"{max_num_retries=} must be greater than 0"
        assert backoff_factor > 1, f"{backoff_factor=} must be greater than 1"
        wait_time = self.min_time_between_calls
        for i in range(max_num_retries):
            response = session.get(self.url, headers=self.headers, params=params)
            if response.status_code == 200:
                return response
            logging.info(response.content.decode("utf-8").strip())
            if response.status_code not in WAIT_RESPONSE_CODES:
                return response
            if i == max_num_retries - 1:
                break
            time.sleep(wait_time)
            wait_time *= backoff_factor
        return response

    def print_debug_progress(self, i: int, total: int) -> None:
        s = str(i)
        num = ceil(log10(total + 1))
        s = " " * (num - len(s)) + s
        logging.info(f"Yielding object {s}/{total}")

    def execute(self) -> Generator[Dict[str, Any], None, None]:
        params = {**self.params}
        with requests.Session() as session:
            response = self.get_or_retry(session, params)
            if response.status_code != 200:
                params.pop("pageSize")
                response = self.get_or_retry(session, params)
                assert response.status_code == 200, "\n" + response.content.decode()
            jsn = response.json()
            # expects a schema of
            # {"page": int, "total": int, "objects": [...]}
            check_type = isinstance(jsn, dict) and "total" in jsn and "objects" in jsn
            if not check_type:
                yield jsn
                # manually tell Python no more items to iterate
                raise StopIteration
            total = jsn["total"]
            objs = jsn["objects"]
            logging.info(f"{total} objects found")
            total_pages = ceil(total / len(objs))
            self.print_debug_progress(1, total_pages)
            yield objs
            last_called = time.perf_counter()
            for i in range(1, total_pages):
                params["page"] = i
                now = time.perf_counter()
                diff = self.min_time_between_calls - (now - last_called)
                if diff > 0:
                    time.sleep(diff)
                response = self.get_or_retry(session, params)
                assert response.status_code == 200, response.content.decode()
                jsn = response.json()
                objs = jsn["objects"]
                self.print_debug_progress(i + 1, total_pages)
                yield objs
                last_called = time.perf_counter()
