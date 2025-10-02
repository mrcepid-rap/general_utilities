import os
import math
import dxpy

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Optional, List

from general_utilities.job_management.joblauncher_interface import JobLauncherInterface, JobInfo


class ThreadUtility(JobLauncherInterface):
    """Class for managing the execution of functions using a ThreadPoolExecutor.

    This class implements the JobLauncherInterface and provides methods to launch jobs. Briefly, a worked example of
    how to use this class is as follows::

        # Dummy test method for submission:
        def test_method(input_str: str, input_int: int, input_bool: bool) -> Tuple[str, int, bool]:
            my_str = input_str
            my_int = input_int
            my_bool = input_bool
            return my_str, my_int, my_bool

        # Construct ThreadUtility
        thread_utility = ThreadUtility(threads=8, thread_factor=1, incrementor=1)

        # Submit a job to ThreadUtility, but does not run it yet:
        thread_utility.launch_job(test_method,
            inputs={'input_str':'Hello', 'input_int':42, 'input_bool':True},
            outputs=['output_str', 'output_int', 'output_bool'])

        # Submit all queued jobs to the ThreadPoolExecutor and monitor their execution:
        thread_utility.submit_and_monitor()

        # Retrieve the results of the completed jobs. These outputs will be formatted as a dictionary based on the 'outputs'
        # parameter provided using :func:`launch_job`.
        for result in thread_utility:
            print(result)
            # For the above, will print: {'output_str': 'Hello', 'output_int': 42, 'output_bool': True}

    If trying to decide on additional VM resources rather than threads, it may be preferred to use the joblauncher_factory
    method provided in :func:`general_utilities.job_management.joblauncher_factory` to decide between using SubjobUtility
    (for recruitment of additional machines) or ThreadUtility (for local jobs).

    :param incrementor: The incrementor for job submission, default is 500.
    :param threads: The number of threads available on the machine. If None, will use os.cpu_count().
    :param thread_factor: The number of threads required per job in this thread pool, default is 1.
    """

    def __init__(self,
                 incrementor: int = 500,
                 threads: int = None,
                 thread_factor: int = 1):

        super().__init__(incrementor=incrementor,
                         concurrent_job_limit=self._decide_concurrent_job_limit(threads, thread_factor))

        self._executor = ThreadPoolExecutor(max_workers=self._concurrent_job_limit)

    def launch_job(self,
                   function: Callable,
                   inputs: Optional[Dict[str, Any]] = None,
                   outputs: Optional[List[str]] = None,
                   **kwargs) -> None:
        """
        Queue a job for later submission, harmonized with SubjobUtility.

        This method will run the requested function and then, using 'outputs' as a guide, will format the output
        as a dictionary. For example, given a function that returns two values, if outputs = ['output1', 'output2'],
        the returned dictionary will be {'output1': value1, 'output2': value2}. Note that Tuple returns will be
        treated as separate objects, while List and Dict returns will be treated as single objects. For example,

        Given a return type of List[Any]:

        with outputs = ['output_list'], the returned dictionary will be {'output_list': [val1, val2, ...]}.

        Given a return type of Tuple[List, List]:

        with outputs = ['output1', 'output2'], the returned dictionary will be {'output1': list1, 'output2': list2}.

        :param function: The function to be executed in the thread.
        :param inputs: A dictionary of input parameters to be passed to the function.
        :param outputs: A named dictionary of output parameters from the function being run
            (e.g. outputs = {'output': 5}).
            This mirrors SubJobUtility functionality.
        :param kwargs: Additional keyword arguments (not used in this implementation).
        """
        if self._queue_closed:
            raise dxpy.AppError("Thread executor has already been collected from!")

        self._total_jobs += 1

        if outputs is None:
            outputs = []

        # Create job info dictionary
        input_parameters: JobInfo = {
            'function': function,
            'properties': {},
            'input': inputs,
            'outputs': outputs,
            'job_type': None,
            'destination': None,
            'name': None,
            'instance_type': None
        }

        # Append job info to the job queue
        self._job_queue.append(input_parameters)

    def submit_and_monitor(self) -> None:
        """
        Submit the queued jobs and monitor their execution.
        """
        if not self._job_queue:
            raise dxpy.AppError('No jobs submitted to future pool!')

        self._queue_closed = True

        self._logger.info(f'{"Total number of threads to iterate through":{65}}: {self._total_jobs}')

        futures_list = []
        # Submit each job in the queue to the executor
        for job in self._job_queue:

            # Submit the job to the executor
            submission = self._executor.submit(self._run_requested_function, job['function'], job['input'], job['outputs'])
            # Append the future object to the futures list
            futures_list.append(submission)

        # Monitor the completion of the submitted jobs
        for output in futures.as_completed(futures_list):
            # Append the result of each completed job to the output array
            self._output_array.append(output.result())
            self._num_completed_jobs += 1
            self._print_status()

    @staticmethod
    def _run_requested_function(function: Callable, inputs: Optional[Dict[str, Any]], outputs: List[str]) -> Optional[Dict[str, Any]]:
        """Helper class that wraps the requested function and formats the output, when complete, as a dictionary.

        This method will run the requested function and then, using 'outputs' as a guide, will format the output
        as a dictionary. For example, given a function that returns two values, if outputs = ['output1', 'output2'],
        the returned dictionary will be {'output1': value1, 'output2': value2}.

        :param function: The function to be executed in the thread.
        :param inputs: A dictionary of input parameters to be passed to the function.
        :param outputs: A named dictionary of output parameters from the function being run
        :returns: A dictionary of outputs, labelled by the names provided in the outputs parameter. If outputs is None,
            then None is returned to match expected output from the function.
        """

        function_outputs = function(**inputs)
        if len(outputs) == 0:
            return_dict = None
        else:
            # Case 1: function already returned a dict with matching keys
            if isinstance(function_outputs, dict):
                return_dict = function_outputs

                # sanity check: make sure all expected labels are present
                missing = [o for o in outputs if o not in return_dict]
                if missing:
                    raise ValueError(
                        f"Function returned a dict missing expected keys: {missing}"
                    )

            # Case 2: function returned a single value
            elif not isinstance(function_outputs, tuple):
                function_outputs = (function_outputs,)
                return_dict = {outputs[0]: function_outputs[0]}

            # Case 3: function returned a tuple of values
            else:
                if len(function_outputs) != len(outputs):
                    raise ValueError(
                        f"Function returned {len(function_outputs)} values, "
                        f"but {len(outputs)} output labels were provided!"
                    )
                return_dict = {
                    output_label: function_outputs[n]
                    for n, output_label in enumerate(outputs)
                }

        return return_dict

    def _decide_concurrent_job_limit(self, requested_threads: int, thread_factor: int) -> int:
        """
        Get the number of concurrent jobs that can be run at one time using either the provided number of threads or the
        number of threads available on the machine.

        :param requested_threads: The number of threads requested by the user.
        :param thread_factor: The number of threads required per job in this thread pool.
        :return: The number of concurrent jobs that can be used on this machine
        """

        threads = requested_threads if requested_threads else os.cpu_count()
        if threads is None or threads < 1:
            self._logger.error('Not enough threads on machine to complete task. Number of threads on this machine '
                               f'({threads}) is less than 1.')
            raise ValueError('Not enough threads on machine to complete task.')

        available_workers = math.floor(threads / thread_factor)
        if available_workers < 1:
            self._logger.error('Not enough threads on machine to complete task. Number of threads on this machine '
                               f'({threads}) is less than {thread_factor}.')
            raise ValueError('Not enough threads on machine to complete task.')

        return available_workers
