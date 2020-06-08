import logging
import subprocess
from subprocess import CalledProcessError
import re
from typing import Callable
import traceback
import sys
import time

from openeogeotrellis.job_registry import JobRegistry

_log = logging.getLogger(__name__)

# TODO: make this job tracker logic an internal implementation detail of JobRegistry?

class JobTracker:
    class _UnknownApplicationIdException(ValueError):
        pass

    def __init__(self, job_registry: Callable[[], JobRegistry], principal: str, keytab: str):
        self._job_registry = job_registry
        self._principal = principal
        self._keytab = keytab
        self._track_interval = 60  # seconds

    def update_statuses(self) -> None:
        try:
            i = 0

            while True:
                try:
                    if i % 60 == 0:
                        self._refresh_kerberos_tgt()

                    with self._job_registry() as registry:
                        print("tracking statuses...")

                        jobs_to_track = registry.get_running_jobs()

                        for job in jobs_to_track:
                            job_id, user_id = job['job_id'], job['user_id']
                            application_id, current_status = job['application_id'], job['status']

                            if application_id:
                                try:
                                    state, final_state = JobTracker._yarn_status(application_id)
                                    new_status = JobTracker._to_openeo_status(state, final_state)

                                    if current_status != new_status:
                                        registry.set_status(job_id, user_id, new_status)
                                        print("changed job %s status from %s to %s" % (job_id, current_status, new_status))

                                    if final_state != "UNDEFINED":
                                        registry.mark_done(job_id, user_id)
                                        print("marked %s as done" % job_id)
                                except JobTracker._UnknownApplicationIdException:
                                    registry.mark_done(job_id, user_id)
                except Exception:
                    traceback.print_exc(file=sys.stderr)

                time.sleep(self._track_interval)

                i += 1
        except KeyboardInterrupt:
            pass

    @staticmethod
    def yarn_available() -> bool:
        """Check if YARN tools are available."""
        try:
            _log.info("Checking if Hadoop 'yarn' tool is available")
            output = subprocess.check_output(["yarn", "version"]).decode("ascii")
            hadoop_yarn_available = "hadoop" in output.lower()
            _log.info("Hadoop yarn available: {a}".format(a=hadoop_yarn_available))
            return hadoop_yarn_available
        except Exception as e:
            _log.info("Failed to run 'yarn': {e!r}".format(e=e))
            return False

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
            stdout = e.stdout.decode()
            if "doesn't exist in RM or Timeline Server" in stdout:
                raise JobTracker._UnknownApplicationIdException(stdout)
            else:
                raise

    @staticmethod
    def _to_openeo_status(state: str, final_state: str) -> str:
        if state == 'ACCEPTED':
            new_status = 'queued'
        elif state == 'RUNNING':
            new_status = 'running'
        else:
            new_status = 'created'

        if final_state == 'KILLED':
            new_status = 'canceled'
        elif final_state == 'SUCCEEDED':
            new_status = 'finished'
        elif final_state == 'FAILED':
            new_status = 'error'

        return new_status

    def _refresh_kerberos_tgt(self):
        if self._keytab and self._principal:
            subprocess.check_call(["kinit", "-kt", self._keytab, self._principal])
        else:
            _log.warn("No Kerberos principal/keytab: will not refresh TGT")


if __name__ == '__main__':
    JobTracker(JobRegistry, 'vdboschj', "/home/bossie/Documents/VITO/vdboschj.keytab").update_statuses()
