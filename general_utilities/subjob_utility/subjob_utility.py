from time import sleep

import dxpy
import dxpy.api

from general_utilities.association_resources import run_cmd


class SubjobUtility:

    def __init__(self, concurrent_job_limit: int = 100):

        self._concurrent_job_limit = concurrent_job_limit

        self._job_queue = []
        self._num_jobs = 0
        self._queue_closed = False
        pass

    def launch_job(self, function_name: str, inputs: dict, outputs: dict, instance_type: str = None) -> None:

        if self._queue_closed is True:
            raise dxpy.AppError('Cannot submit new subjobs after calling monitor_subjobs()!')

        input_parameters = {'function': function_name,
                            'input': inputs,
                            'outputs': outputs}
        if instance_type is not None:  # This might work...
            input_parameters['systemRequirements'] = {"*": {"instanceType": instance_type}}

        self._job_queue.append(input_parameters)
        self._num_jobs += 1

    def monitor_subjobs(self):

        self._queue_closed = True

        can_submit = True
        job_ids = []
        current_jobs = 0
        for job in self._job_queue:

            # Make sure the queue isn't full...
            while can_submit is False:
                if current_jobs <= self._concurrent_job_limit:
                    can_submit = True
                else:
                    self._monitor_submitted(job_ids)
                    sleep(1)

            current_jobs += 1
            launched_job = dxpy.api.job_new(input_params=job,
                                            always_retry=False)
            print(launched_job)
            job_ids.append(launched_job['id'])

        sleep(60*10)
        run_cmd('ls ./', livestream_out=True)

    def _monitor_submitted(self, job_ids: list):
        pass


