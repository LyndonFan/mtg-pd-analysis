import requests
from math import ceil, log10
from typing import Any, Dict, List, Generator
import time

import logging


def get_or_retry(
    session: requests.Session,
    url: str,
    start_wait_time: float = 0.5,
    max_num_retries: int = 7,
    headers: Dict[str, Any] = {},
    params: Dict[str, Any] = {},
    wait_response_codes: List[int] = [429, 503, 504],
) -> requests.Response:
    wait_time = start_wait_time
    for i in range(max_num_retries):
        response = session.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response
        logging.debug(response.content.decode("utf-8").strip())
        if response.status_code not in wait_response_codes:
            return response
        if i == max_num_retries - 1:
            break
        time.sleep(wait_time)
        wait_time *= 2
    return response


def get_paginate_response(
    url: str,
    page_size: int = 500,
    min_time_between_calls: float = 0.5,
    headers: Dict[str, Any] = {},
    params: Dict[str, Any] = {},
) -> Generator[List[Dict[str, Any]], None, None]:
    params.pop("page", None)
    if "pageSize" not in params:
        params["pageSize"] = page_size
    with requests.Session() as session:
        response = get_or_retry(
            session,
            url,
            start_wait_time=min_time_between_calls,
            headers=headers,
            params=params,
        )
        if response.status_code != 200:
            params.pop("pageSize")
            response = get_or_retry(
                session,
                url,
                start_wait_time=min_time_between_calls,
                headers=headers,
                params=params,
            )
            assert response.status_code == 200, "\n" + response.content.decode()
        jsn = response.json()
        # expects a schema of
        # {"page": int, "total": int, "objects": [...]}
        total = jsn["total"]
        objs = jsn["objects"]
        logging.info(f"{total} objects found")
        total_n = ceil(total / len(objs))
        num = ceil(log10(total_n + 1))
        logging.info(f"Yielding object {' '*(num-1)}1/{total_n}")
        yield objs
        last_called = time.perf_counter()
        for i in range(1, total_n):
            params["page"] = i
            now = time.perf_counter()
            diff = min_time_between_calls - (now - last_called)
            if diff > 0:
                time.sleep(diff)
            response = get_or_retry(
                session,
                url,
                start_wait_time=min_time_between_calls,
                headers=headers,
                params=params,
            )
            assert response.status_code == 200, response.content.decode()
            jsn = response.json()
            objs = jsn["objects"]
            s = str(i + 1)
            s = " " * (num - len(s)) + s
            logging.info(f"Yielding object {s}/{total_n}")
            yield objs
            last_called = time.perf_counter()
