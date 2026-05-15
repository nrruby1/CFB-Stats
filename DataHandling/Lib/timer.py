import time
import logging
from typing import Any, Callable

log = logging.getLogger("CfbStats")


class Timer:
    def __init__(self, name: str):
        self.name = name
        self.start_time = time.perf_counter()
        self.stop_time = None

    def start(self):
        self.start_time = time.perf_counter()

    def stop(self) -> str:
        self.stop_time = time.perf_counter()
        elapsed_time = self.stop_time - self.start_time
        return f"{self.name} took {elapsed_time:.2f} seconds"

    def stop_and_log(self, level=logging.DEBUG):
        log.log(level, self.stop())

    def get_elapsed_time(self) -> float:
        if self.stop_time is not None:
            return self.stop_time - self.start_time
        else:
            return time.perf_counter() - self.start_time

    def run(self, func: Callable, level=logging.DEBUG) -> Any:
        self.start()
        result = func()
        self.stop_and_log(level)
        return result
