import math
import os
from importlib import import_module

import dxpy
import inspect

from enum import Enum, auto
from time import sleep
from datetime import datetime
from typing import TypedDict, Dict, Any, List, Iterator, Optional, Callable

from general_utilities.mrc_logger import MRCLogger


class Environment(Enum):

    DX = dxpy.DXJob
    LOCAL = dxpy.DXApplet


class DXJobInfo(TypedDict):
    function: str
    properties: Dict[str, str]
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
    TERMINATING = RunningStatus.RUNNING
    TERMINATED = RunningStatus.FAILED
    FAILED = RunningStatus.FAILED


class SubjobUtility:
    """A class that contains information on, launches, and monitors subjobs on the DNANexus platform.

    This class functions in two ways, depending on the methods used to queue jobs:

    1. If run from a local machine (e.g., a macbook) via :func:launch_applet() – Will launch new jobs that are
    not dependent on any current running job

    2. If run from a currently running DNANexus job via :func:launch_job() – Will launch subjobs that are
    dependent on the current job to be run via the DXJob class

    See individual method documentation for more information, but briefly, the workflow for using this class is
    the following:

    1. Queue jobs using either the :func:launch_applet() or :func:launch_job(). Do NOT use both with the same
    constructor (there are checks to prevent this)!

    2. Submit jobs that have been queued with :func:submit_queue(). After calling this method, the queue closes
    and new jobs can no longer be added to prevent iteration errors when collecting job output.

    3. Collect jobs using the built-in iterator (specified by the :func:__iter__() dunder method).

    A brief example follows::

        # Call this class with the default constructor
        subjob_utility = SubjobUtility()

        # Add jobs to the queue
        phenotype = 'cardiac_arrest'
        for chromosome in range(1,23):
            subjob_utility.launch_job(function=burden_interaction,
                                      inputs={'chromosome': chromosome,
                                              'pheno_name': phenotype},
                                      outputs=['outfile'],
                                      instance_type='mem3_ssd1_v2_x8',
                                      name=f'{chromosome}_{phenotype}_subjob')

        # Launch jobs on DNANexus
        subjob_utility.submit_queue()

        # Collect outputs:
        outputs = []
        for output in subjob_utility:
            # Each output is stored as dxpy.dxlink() dict in a separate item within the 'output' list
            for subjob_output in output:
                link = subjob_output['$dnanexus_link']
                field = link['field']
                field_value = dxpy.DXJob(link['job']).describe()['output'][field]
                print(f'Output for {field}: {field_value}')
                outputs.append(field_value)

    :param concurrent_job_limit: Number of jobs that can be run at once. Default of 100 is the actual limit for
        concurrent jobs on the DNANexus platform. It is also wishful in that you will rarely be able to have 100
        jobs simultaneously running. [100]
    :param retries: Number of times to retry a job before marking it as a fail. Default is intended to let jobs
        interrupted by the cloud provider to restart, NOT to fix broken code. [1]
    :param incrementor: This class will print a status method for every :param:incrementor jobs completed with a
        percentage of total jobs completed. [500]
    :param log_update_time: How often should the log of current jobs be printed in seconds. [60]
    """

    def __init__(self, concurrent_job_limit: int = 100, retries: int = 1, incrementor: int = 500,
                 log_update_time: int = 60):

        self._logger = MRCLogger(__name__).get_logger()

        # Dereference class parameters
        self._concurrent_job_limit = concurrent_job_limit
        self._incrementor = incrementor
        self._log_update_time = log_update_time

        # We define three difference queues for use during runtime:
        # job_queue   – jobs waiting to be submitted
        # job_running – jobs currently running, with a dict keyed on the DX job-id and with a value of DXJobDict class
        #   which contains the job class from instantiation and information about the job
        #   and information about the job
        # job_failed  – A list of jobs which failed during runtime which, if the job has additional retries, can
        #   be resubmitted
        self._job_queue: List[DXJobInfo] = []
        self._job_running: Dict[str, DXJobDict] = dict()
        self._job_failed: List[DXJobInfo] = []

        # Job type & count monitoring
        self._queue_type: Optional[Environment] = None
        self._queue_closed = False
        self._total_jobs = 0
        self._num_completed_jobs = 0

        # Set default job instance type
        self._retries = retries
        if 'DX_JOB_ID' in os.environ:
            parent_job = dxpy.DXJob(dxid=os.getenv('DX_JOB_ID'))
            self._default_instance_type = parent_job.describe(fields={'systemRequirements': True})['systemRequirements']['*']['instanceType']
        else:
            self._default_instance_type = None

        # Manage returned outputs
        self._output_array = []

    def __iter__(self) -> Iterator[List[dict]]:
        """Return an iterator over the outputs collected when jobs finish.

        Outputs returned by the class are a list of dictionaries formatted like dxpy.dxlinks:

        output = {'$dnanexus_link': {'field': output_name}}

        The only way to recover the ACTUAL output is to use something like::

            output = dxpy.DXJob(link['job']).describe()['output'][output['$dnanexus_link']['field']

        This will query the job for the actual output. This is not my fault...

        :return: An iterator of output references
        """
        return iter(self._output_array)

    def __len__(self) -> int:
        """Returns the number of outputs currently in the output queue

        :return: The number of outputs currently in the output queue
        """
        return len(self._output_array)

    def launch_applet(self, applet_hash: str, inputs: Dict[str, Any], outputs: List[str] = None,
                      destination: str = None, instance_type: str = None, name: str = None) -> None:
        """Launch a DNANexus job with the given parameters from a LOCAL machine.

        The only required parameters for this function are :param:applet_hash and :param:inputs. This method will add
        a job with input parameters provided by this method call to the class :param:self._job_queue.

        DO NOT use this method if operating within a current DNANexus job. There may be unforeseen consequences...

        :param applet_hash: The applet hash for the applet that should be run (e.g., applet-1234567890ABCDEFGabcdefg)
        :param inputs: The function inputs. These must include all default inputs and be identical to those defined
            in the dxapp.json inputs section.
        :param outputs: The function outputs. These must include all default outputs and be identical to those defined
            in the dxapp.json outputs section. May be 'None' if there are no defined outputs from the given
            applet. [None]
        :param destination: Where should outputs be placed on the DNANexus platform. Default places outputs in the root
            level directory for the executing project (e.g., '/'). [None]
        :param instance_type: What instance type should be used? Default sets the instance type based on the
            'instance_type' specification in the dxapp.json. [None]
        :param name: Name of the job. Default names the job after the executing applet name. [None]
        """

        # Check if the queue has been closed by submit_queue()
        if self._queue_closed is True:
            raise dxpy.AppError('Cannot submit new subjobs after calling monitor_subjobs()!')

        # Make sure only identical job types have been launched
        if self._queue_type is Environment.DX:
            raise dxpy.AppError('Cannot mix jobtypes between launch_applet() and launch_job()!')
        elif self._queue_type is None:
            self._queue_type = Environment.LOCAL

        self._total_jobs += 1

        if outputs is None:
            outputs: List[str] = []

        input_parameters: DXJobInfo = {'function': applet_hash,
                                       'properties': {},
                                       'input': inputs,
                                       'outputs': outputs,
                                       'job_type': Environment.LOCAL,
                                       'retries': 0,
                                       'destination': f'/{destination}',
                                       'name': f'subjob_{self._total_jobs}' if name is None else None,
                                       'instance_type': instance_type if instance_type else self._default_instance_type}

        self._job_queue.append(input_parameters)

    def launch_job(self, function: Callable, inputs: Dict[str, Any], outputs: List[str] = None,
                   instance_type: str = None, name: str = None) -> None:
        """Launch a DNANexus job with the given parameters from a REMOTE machine.

        The only required parameters for this function are :param:applet_hash and :param:inputs. This method will add
        a job with input parameters provided by this method call to the class :param:self._job_queue.

        DO NOT use this method if operating within a current DNANexus job. There may be unforeseen consequences...


        :param function:
        :param inputs:
        :param outputs:
        :param instance_type:
        :param name:
        :return:
        """

        # Check if the queue has been closed by submit_queue()
        if self._queue_closed is True:
            raise dxpy.AppError('Cannot submit new subjobs after calling monitor_subjobs()!')

        # Make sure only identical job types have been launched
        if self._queue_type is Environment.LOCAL:
            raise dxpy.AppError('Cannot mix jobtypes between launch_applet() and launch_job()!')
        elif self._queue_type is None:
            self._queue_type = Environment.DX

        self._total_jobs += 1

        if outputs is None:
            outputs: List[str] = []

        # For future reference, there is an entire logic for why we pass the function to the method rather than the
        # string representation of the method's name.
        # 1. To be able to 'see' the methods of other files / packages decorated with dxpy.entry_point(), the class has
        #    to be imported into the calling file (e.g. 'import from'). Using the string representation makes the python
        #    interpreter / pycharm think that the import isn't used, so we use the actual function.
        # 2. We then convert to the string representation below because the DNANexus DXJob call requires a string
        #    representation.
        input_parameters: DXJobInfo = {'function': function.__name__,
                                       'properties': {'module': inspect.getmodule(function).__name__},
                                       'input': inputs,
                                       'outputs': outputs,
                                       'job_type': Environment.DX,
                                       'retries': 0,
                                       'destination': None,
                                       'name': None if name is None else name,
                                       'instance_type': instance_type if instance_type else self._default_instance_type}

        self._job_queue.append(input_parameters)

    def submit_queue(self):

        # Close the queue to future job submissions to save my sanity for weird edge cases
        self._queue_closed = True

        self._logger.info("{0:65}: {val}".format("Total number of jobs to iterate through", val=self._total_jobs))

        # Keep going until we get every job submitted or finished...
        while len(self._job_queue) > 0 or len(self._job_running.keys()) > 0:
            self._print_status()
            self._monitor_subjobs()
            if len(self._job_running.keys()) > 0:
                sleep(self._log_update_time)

        if len(self._job_failed) > 0:
            self._logger.info('All jobs completed, printing failed jobs...')
            for failed_job in self._job_failed:
                self._logger.error(f'FAILED: {failed_job}')
        else:
            self._logger.info('All jobs completed, No failed jobs...')

    def _print_status(self):
        self._logger.info(f'{"Jobs currently in the queue":{65}}: {len(self._job_queue)}')
        self._logger.info(f'{"Jobs currently running":{65}}: {len(self._job_running.keys())}')
        self._logger.info(f'{"Jobs failed":{65}}: {len(self._job_failed)}')

        curr_time = datetime.today()
        self._logger.info(f'{curr_time.isoformat("|", "seconds"):{"-"}^{65}}')

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
                dxjob.new(fn_input=job['input'], fn_name=job['function'], instance_type=job['instance_type'],
                          properties=job['properties'], name=job['name'])

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
                self._logger.info(
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


def check_subjob_decorator() -> Optional[str]:
    """This class checks to see if the dx Applet is actually a subjob being run on the DNANexus platform.

    To be able to run subjobs on DNANexus, the class / method MUST be imported using standard python imports (e.g.,
    import ... from ...) by the instantiating file that contains the dxpy.entry_point('main') decorator. This means
    that modules (e.g., 'burden') that are dynamically loaded WILL NOT be found in the python classpath prior to
    DNANexus attempting to launch the subjob with the indicated dxpy.entry_point() decorator. To solve this, we do two
    things:

    1. For all subjobs being launched, we add a property to the job indicating where this method
    (check_subjob_decorator) should look for the decorated method (see DXJobInfo in this file for more information).

    2. When the new subjob is launched, we then run this method before dxpy.run() is called to make sure that the
    decorated method is properly included in the dxpy.utils.exec_utils.ENTRY_POINT_TABLE dict that tells the DNANexus
    job handler what function to run at startup. This approach uses the import_module function from importlib to
    dynamically load the requested methods into the python classpath

    :return: The name of the identified module that was loaded by this method or None if no module was loaded
    """

    loaded_module = None
    job = dxpy.DXJob(dxpy.JOB_ID)
    if 'module' in job.describe()['properties']:
        loaded_module = job.describe()['properties']['module']
        try:
            import_module(loaded_module)
        except ModuleNotFoundError:
            raise

    return loaded_module



