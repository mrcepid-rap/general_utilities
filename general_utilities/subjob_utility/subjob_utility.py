import logging
import math
import os
import dxpy
import dxpy.api

from enum import Enum, auto
from time import sleep
from datetime import datetime
from typing import TypedDict, Dict, Any, List, Iterator, Optional


class Environment(Enum):

    DX = dxpy.DXJob
    LOCAL = dxpy.DXApplet


class DXJobInfo(TypedDict):
    function: str
    input: Dict[str, Any]
    outputs: List[str]
    job_type: Environment
    retries: int
    destination: Optional[str]
    name: Optional[str]
    instance_type: Optional[str]


class DXJobDict(TypedDict):
    job_class: dxpy.DXJob
    job_info: DXJobInfo


class RunningStatus(Enum):
    # DO NOT remove the '()' after auto, even though pycharm says it is wrong. IT IS NOT WRONG.
    COMPLETE = auto()
    RUNNING = auto()
    FAILED = auto()


class JobStatus(Enum):
    DONE = RunningStatus.COMPLETE
    IDLE = RunningStatus.RUNNING
    RUNNABLE = RunningStatus.RUNNING
    RUNNING = RunningStatus.RUNNING
    WAITING_ON_OUTPUT = RunningStatus.RUNNING
    WAITING_ON_INPUT = RunningStatus.RUNNING
    TERMINATED = RunningStatus.FAILED
    FAILED = RunningStatus.FAILED


class SubjobUtility:

    def __init__(self, concurrent_job_limit: int = 100, retries: int = 1, incrementor: int = 500):

        self._concurrent_job_limit = concurrent_job_limit
        self._incrementor = incrementor

        # Make a queue for job submission and monitoring
        self._job_queue: List[DXJobInfo] = []
        self._job_running: Dict[str, DXJobDict] = dict()
        self._job_failed: List[DXJobInfo] = []

        # Job count monitoring
        self._queue_closed = False
        self._total_jobs = 0
        self._num_completed_jobs = 0

        # Set default job parameters
        self._retries = retries
        if 'DX_JOB_ID' in os.environ:
            parent_job = dxpy.DXJob(dxid=os.getenv('DX_JOB_ID'))
            self._default_instance_type = parent_job.describe(fields={'systemRequirements': True})['systemRequirements']['*']['instanceType']
        else:
            self._default_instance_type = None

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

    def launch_applet(self, applet_hash: str, inputs: Dict[str, Any], outputs: List[str] = None,
                      destination: str = None, instance_type: str = None, name: str = None):

        if self._queue_closed is True:
            raise dxpy.AppError('Cannot submit new subjobs after calling monitor_subjobs()!')

        self._total_jobs += 1

        if outputs is None:
            outputs: List[str] = []

        input_parameters: DXJobInfo = {'function': applet_hash,
                                       'input': inputs,
                                       'outputs': outputs,
                                       'job_type': Environment.LOCAL,
                                       'retries': 0,
                                       'destination': f'/{destination}',
                                       'name': f'subjob_{self._total_jobs}' if name is None else None,
                                       'instance_type': self._default_instance_type if instance_type is None else instance_type}

        self._job_queue.append(input_parameters)

    def launch_job(self, function_name: str, inputs: Dict[str, Any], outputs: List[str] = None,
                   instance_type: str = None, name: str = None) -> None:

        if self._queue_closed is True:
            raise dxpy.AppError('Cannot submit new subjobs after calling monitor_subjobs()!')

        self._total_jobs += 1

        if outputs is None:
            outputs: List[str] = []

        input_parameters: DXJobInfo = {'function': function_name,
                                       'input': inputs,
                                       'outputs': outputs,
                                       'job_type': Environment.DX,
                                       'retries': 0,
                                       'destination': None,
                                       'name': None if name is None else name,
                                       'instance_type': self._default_instance_type if instance_type is None else instance_type}

        self._job_queue.append(input_parameters)

    def submit_queue(self):

        # Close the queue to future job submissions to save my sanity for weird edge cases
        self._queue_closed = True

        print("{0:65}: {val}".format("Total number of jobs to iterate through", val=self._total_jobs))

        # Keep going until we get every job submitted or finished...
        while len(self._job_queue) > 0 or len(self._job_running.keys()) > 0:
            self._print_status()
            self._monitor_subjobs()
            sleep(60)

        if len(self._job_failed) > 0:
            print('All jobs completed, printing failed jobs...')
            for failed_job in self._job_failed:
                print(f'FAILED: {failed_job}')
        else:
            print('All jobs completed, No failed jobs...')

    def _print_status(self):
        print(f'{"Jobs currently in the queue":{65}}: {len(self._job_queue)}')
        print(f'{"Jobs currently running":{65}}: {len(self._job_running.keys())}')
        print(f'{"Jobs failed":{65}}: {len(self._job_failed)}')

        curr_time = datetime.today()
        print(f'{curr_time.isoformat("|", "seconds"):{"-"}^{65}}')

    def _monitor_subjobs(self) -> None:

        # Set a boolean for allowing to submit jobs UNTIL we hit the job limit (defined by self._concurrent_job_limit)
        can_submit = True

        while len(self._job_queue) > 0:

            job = self._job_queue.pop()

            # Make sure the queue isn't full...
            while can_submit is False:
                if len(self._job_running.keys()) < self._concurrent_job_limit:
                    can_submit = True

                self._print_status()
                self._monitor_submitted()
                sleep(60)

            if job['job_type'] == Environment.DX:
                # A bit strange, but this enum returns a class that we can instantiate for our specific use-case
                dxjob = job['job_type'].value()
                dxjob = dxjob.new(fn_input=job['input'], fn_name=job['function'], instance_type=job['instance_type'])

            elif job['job_type'] == Environment.LOCAL:
                dxapplet = job['job_type'].value(job['function'])
                dxjob = dxapplet.run(applet_input=job['input'], folder=job['destination'], name=job['name'],
                                     instance_type=job['instance_type'], priority='low')
            else:
                raise RuntimeError('Job does not have type DX or LOCAL, which should be impossible')

            self._job_running[dxjob.describe(fields={'id': True})['id']] = {'job_class': dxjob,
                                                                            'job_info': job}

            if len(self._job_running.keys()) >= self._concurrent_job_limit:
                can_submit = False

        self._monitor_submitted()

    def _check_job_status(self, job: DXJobDict) -> RunningStatus:

        description = job['job_class'].describe(fields={'state': True})
        curr_status = JobStatus[description['state'].rstrip().upper()]
        if curr_status.value == RunningStatus.COMPLETE:
            output_list = []
            for output in job['job_info']['outputs']:
                output_list.append(job['job_class'].get_output_ref(output))
            self._output_array.append(output_list)

            self._num_completed_jobs += 1
            if math.remainder(self._num_completed_jobs, self._incrementor) == 0:
                print(
                    f'{"Total number of jobs finished":{65}}: {self._num_completed_jobs} / {self._total_jobs} '
                    f'({((self._num_completed_jobs / self._total_jobs) * 100):0.2f}%)')

        return curr_status.value

    def _monitor_submitted(self) -> None:

        curr_keys = list(self._job_running.keys())
        for job_id in curr_keys:
            job_status = self._check_job_status(self._job_running[job_id])
            if job_status is RunningStatus.COMPLETE:
                del self._job_running[job_id]
            elif job_status is RunningStatus.FAILED:
                job = self._job_running[job_id]['job_info']
                del self._job_running[job_id]
                if job['retries'] < self._retries:
                    job['retries'] += 1
                    self._job_queue.append(job)
                else:
                    self._job_failed.append(job)
