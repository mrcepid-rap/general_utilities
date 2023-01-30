import logging
import math
import os
from enum import Enum
from time import sleep

import dxpy
import dxpy.api

from typing import TypedDict, Dict, Any, List, Iterator


class DXJobDict(TypedDict):
    finished: bool
    job_class: dxpy.DXJob
    retries: int
    outputs: List[str]


class JobStatus(Enum):
    DONE = True
    IDLE = False
    RUNNABLE = False
    RUNNING = False
    WAITING_ON_OUTPUT = False
    WAITING_ON_INPUT = False
    TERMINATED = None
    FAILED = None


class SubjobUtility:

    def __init__(self, concurrent_job_limit: int = 100, retries: int = 1, incrementor: int = 500):

        self._concurrent_job_limit = concurrent_job_limit
        self._incrementor = incrementor

        # Make a queue for job submission
        self._job_queue = []

        # Job count monitoring
        self._queue_closed = False
        self._total_jobs = 0
        self._current_running_jobs = 0
        self._num_completed_jobs = 0

        # Set default job parameters
        self._retries = retries  # TODO: Implement retries...
        parent_job = dxpy.DXJob(dxid=os.getenv('DX_JOB_ID'))
        self._default_instance_type = parent_job.describe(fields={'systemRequirements': True})['systemRequirements']['*']['instanceType']

        # Manage returned outputs
        self._output_array = []

        # Ensure logging handled
        if 'DX_JOB_ID' in os.environ:
            logging.getLogger().addHandler(dxpy.DXLogHandler())
        else:
            logging.basicConfig(level=logging.INFO)

    def __iter__(self) -> Iterator:
        return iter(self._output_array)

    def __len__(self):
        return len(self._output_array)

    def launch_job(self, function_name: str, inputs: Dict[str, Any], outputs: List[str] = None,
                   instance_type: str = None) -> None:

        if outputs is None:
            outputs = []
        if self._queue_closed is True:
            raise dxpy.AppError('Cannot submit new subjobs after calling monitor_subjobs()!')

        input_parameters = {'function': function_name,
                            'input': inputs,
                            'outputs': outputs}

        if instance_type is None:  # This might work...
            input_parameters['instance_type'] = self._default_instance_type
        else:
            input_parameters['instance_type'] = instance_type

        self._job_queue.append(input_parameters)
        self._total_jobs += 1

    def monitor_subjobs(self) -> None:

        # Close the queue to future job submissions to save my sanity for weird edge cases
        self._queue_closed = True

        print("{0:65}: {val}".format("Total number of threads to iterate through", val=self._total_jobs))

        # Set a boolean for allowing to submit jobs UNTILL we hit the job limit (defined by self._concurrent_job_limit)
        can_submit = True

        # Store all jobs in a dictionary with specific information about them
        job_ids = dict()
        for job in self._job_queue:

            # Make sure the queue isn't full...
            while can_submit is False:
                if self._current_running_jobs <= self._concurrent_job_limit:
                    can_submit = True

                self._monitor_submitted(job_ids)

            self._current_running_jobs += 1
            dxjob = dxpy.new_dxjob(fn_input=job['input'], fn_name=job['function'], instance_type=job['instance_type'])
            job_ids[dxjob.describe(fields={'id': True})['id']] = {'finished': False, 'job_class': dxjob, 'retries': 0,
                                                                  'outputs': job['outputs']}

            if self._current_running_jobs > self._concurrent_job_limit:
                can_submit = False

        # And monitor until all jobs completed
        self._monitor_completed(job_ids)

    def _check_job_status(self, job: DXJobDict) -> bool:

        description = job['job_class'].describe(fields={'state': True})
        curr_status = JobStatus[description['state'].upper()]
        if curr_status.value:
            output_list = []
            for output in job['outputs']:
                output_list.append(job['job_class'].get_output_ref(output))
            self._output_array.append(output_list)

            self._num_completed_jobs += 1
            if math.remainder(self._num_completed_jobs, self._incrementor) == 0:
                print(
                    f'{"Total number of jobs finished":{65}}: {self._num_completed_jobs} / {self._total_jobs} '
                    f'({((self._num_completed_jobs / self._total_jobs) * 100):0.2f}%)')

            return True
        elif curr_status.value is False:
            return False
        elif curr_status.value is None:
            raise dxpy.AppError(f'A subjob {description["id"]} failed!')

    def _monitor_submitted(self, job_ids: Dict[str, DXJobDict]) -> None:

        can_submit = False
        while can_submit is False:
            for job_id in job_ids.keys():
                if job_ids[job_id]['finished'] is False:
                    job_complete = self._check_job_status(job_ids[job_id])
                    if job_complete:
                        print(f'Job finished: {job_id}')
                        job_ids[job_id]['finished'] = True
                        self._current_running_jobs -= 1

            print(f'Current running jobs (submitted): {self._current_running_jobs}')
            if self._current_running_jobs < self._concurrent_job_limit:
                can_submit = True
            sleep(60)

    def _monitor_completed(self, job_ids: Dict[str, DXJobDict]) -> None:

        all_completed = False
        while all_completed is False:
            for job_id in job_ids.keys():
                if job_ids[job_id]['finished'] is False:
                    job_complete = self._check_job_status(job_ids[job_id])
                    if job_complete:
                        print(f'Job finished: {job_id}')
                        job_ids[job_id]['finished'] = True
                        self._current_running_jobs -= 1

            print(f'Current running jobs (completed): {self._current_running_jobs}')
            if self._current_running_jobs == 0:
                all_completed = True
            sleep(60)


