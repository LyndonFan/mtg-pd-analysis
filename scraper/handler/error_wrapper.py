import logging
import traceback


def error_wrapper(func):
    def inner(*args, **kwargs):
        try:
            res = func(*args, **kwargs)
            return res
        except Exception as e:
            logging.error(f"Encountered {type(e).__name__}:")
            logging.error(e)
            logging.error(traceback.format_exc())

    return inner
