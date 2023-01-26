import math
import dxpy
import logging

from typing import Any, Iterator
from time import sleep
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor


class ThreadUtility:

    def __init__(self, threads: int, error_message: str = "A ThreadUtility thread failed.", incrementor: int = 500,
                 thread_factor: int = 4):

        self._error_message = error_message
        self._incrementor = incrementor
        self._already_collected = False  # A flag to make sure we don't submit jobs to a closed executor
        self._num_jobs = 0
        self._total_finished_models = 0
        available_workers = math.floor(threads / thread_factor)
        self._executor = ThreadPoolExecutor(max_workers=available_workers)
        self._future_pool = []
        logging.basicConfig(level=logging.INFO)

    # I have a feeling the sleep() part is VERY inefficient but I am unsure of how to fix at the moment...
    # Due to how futures work, most of the time a next() call will receive a 'None' return. We need to create a
    # waiter that will hold until we get something OTHER than None, so we can return it to the main class.
    def __next__(self) -> Any:

        curr_future = next(self._future_iterator)
        while curr_future is None:
            curr_future = next(self._future_iterator)
            sleep(0.1)

        self._check_and_format_progress_message()
        result = curr_future.result()
        return result

    def __iter__(self) -> Iterator:

        self._already_collected = True

        if len(self._future_pool) == 0:
            raise dxpy.AppError('No jobs submitted to future pool!')

        logging.info("{0:65}: {val}".format("Total number of threads to iterate through", val=self._num_jobs))

        self._future_iterator = futures.as_completed(self._future_pool)
        return self

    def launch_job(self, class_type, **kwargs) -> None:
        if self._already_collected:
            raise dxpy.AppError("Thread executor has already been collected from!")
        else:
            self._num_jobs += 1
            self._future_pool.append(self._executor.submit(class_type,
                                                           **kwargs))

    # This is a utility method that will essentially 'hold' until all threads added to this class are completed.
    # It just makes it so if one does not need to access the futures, there is no need to implement an empty for loop
    # in your code. Since 'self' represents this class, and this class implements __iter__, it will run the code in
    # the __iter__ class, which will hold until all jobs are completed.
    def collect_futures(self) -> None:
        for _ in self:
            pass

    def _check_and_format_progress_message(self):
        self._total_finished_models += 1
        if math.remainder(self._total_finished_models, self._incrementor) == 0 \
                or self._total_finished_models == self._num_jobs:
            logging.info(f'{"Total number of threads finished":{65}}: {self._total_finished_models} / {self._num_jobs} '
                         f'({((self._total_finished_models / self._num_jobs) * 100):0.2f}%)')
