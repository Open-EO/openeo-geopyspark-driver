import time
import subprocess
from subprocess import CalledProcessError
import re
from typing import Callable
import traceback
import sys

from . import JobRegistry


class JobTracker:
    class _UnknownApplicationIdException(ValueError):
        pass

    def __init__(self, job_registry: Callable[[], JobRegistry]):
        self._job_registry = job_registry

    def update_statuses(self) -> None:
        print("tracking statuses...")

        try:
            while True:
                with self._job_registry() as registry:
                    for job in registry.get_running_jobs():
                        job_id, application_id, current_status = job['job_id'], job['application_id'], job['status']

                        if application_id:
                            try:
                                state, final_state = JobTracker._yarn_status(application_id)
                                new_status = JobTracker._to_openeo_status(state, final_state)

                                if current_status != new_status:
                                    registry.update(job_id, status=new_status)
                                    print("changed job %s status from %s to %s" % (job_id, current_status, new_status))

                                if final_state != "UNDEFINED":
                                    registry.mark_done(job_id)
                                    print("marked %s as done" % job_id)
                            except JobTracker._UnknownApplicationIdException:
                                registry.mark_done(job_id)
                            except Exception:
                                traceback.print_exc(file=sys.stderr)

                time.sleep(30)
        except KeyboardInterrupt:
            pass

    @staticmethod
    def _yarn_status(application_id: str) -> (str, str):
        """Returns (State, Final-State) of a job as reported by YARN."""

        try:
            application_report = subprocess.check_output(
                ["yarn", "application", "-status", application_id]).decode()

            props = re.findall(r"\t(.+) : (.+)", application_report)

            state = next(value for key, value in props if key == 'State')
            final_state = next(value for key, value in props if key == 'Final-State')

            return state, final_state
        except CalledProcessError as e:
            if "doesn't exist in RM or Timeline Server" in e.stdout:
                raise JobTracker._UnknownApplicationIdException(e.stdout)
            else:
                raise

    @staticmethod
    def _to_openeo_status(state: str, final_state: str) -> str:
        if state == 'ACCEPTED':
            new_status = 'queued'
        elif state == 'RUNNING':
            new_status = 'running'
        else:
            new_status = 'submitted'

        if final_state == 'KILLED':
            new_status = 'canceled'
        elif final_state == 'SUCCEEDED':
            new_status = 'finished'
        elif final_state == 'FAILED':
            new_status = 'error'

        return new_status


if __name__ == '__main__':
    JobTracker(JobRegistry).update_statuses()
