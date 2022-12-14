import math
import sys

import dxpy

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor


class ThreadUtility:

    def __init__(self, threads: int, error_message, incrementor=500, thread_factor=4, output_writer=sys.stdout):

        self._error_message = error_message
        self._incrementor = incrementor
        self._output_writer = output_writer
        self._already_collected = False  # A flag to make sure we don't submit jobs to a closed executor
        self._num_jobs = 0
        available_workers = math.floor((threads - 1) / thread_factor)
        self._executor = ThreadPoolExecutor(max_workers=available_workers)
        self._future_pool = []

    def launch_job(self, class_type, **kwargs) -> None:
        if self._already_collected:
            dxpy.AppError("Thread executor has already been collected from!")
        else:
            self._num_jobs += 1
            self._future_pool.append(self._executor.submit(class_type,
                                                           **kwargs))

    def collect_futures(self) -> list:
        self._already_collected = True
        print("{0:65}: {val}".format("Total number of threads to iterate through", val=self._num_jobs),
              file=self._output_writer)
        total_finished_models = 0
        future_results = []
        for future in futures.as_completed(self._future_pool):
            try:
                total_finished_models += 1
                if math.remainder(total_finished_models, self._incrementor) == 0 \
                        or total_finished_models == self._num_jobs:
                    print(f'{"Total number of threads finished":{65}}: {total_finished_models} / {self._num_jobs} '
                          f'({((total_finished_models / self._num_jobs) * 100):0.2f}%)',
                          file=self._output_writer)
                future_results.append(future.result())
            except Exception as err:
                print(self._error_message)
                print(Exception, err)
                raise dxpy.AppError(self._error_message)

        return future_results


