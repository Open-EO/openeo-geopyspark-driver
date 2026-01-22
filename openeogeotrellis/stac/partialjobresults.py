from typing import Optional

from openeo.util import TimingLogger
from openeo_driver.jobregistry import PARTIAL_JOB_STATUS


class PartialJobResults:

    @classmethod
    def get_partial_results_from_load_stac_arguments(cls, arguments, extract_own_job_info, logger_adapter,
                                                     requests_session, supports_async_tasks) -> Optional[dict]:
        url = arguments[0]  # properties will be taken care of @ process graph evaluation time

        if url.startswith("http://") or url.startswith("https://"):
            dependency_job_info = extract_own_job_info(url)
            if dependency_job_info:
                partial_job_status = PARTIAL_JOB_STATUS.for_job_status(dependency_job_info.status)
            else:
                with TimingLogger(f'load_stac({url}): extract "openeo:status"', logger=logger_adapter.debug):
                    with requests_session.get(url, timeout=20) as resp:
                        resp.raise_for_status()
                        stac_object = resp.json()
                    partial_job_status = stac_object.get('openeo:status')
                    logger_adapter.debug(f'load_stac({url}): "openeo:status" is "{partial_job_status}"')

            if supports_async_tasks and partial_job_status == PARTIAL_JOB_STATUS.RUNNING:
                return {
                    'partial_job_results_url': url,
                }
            else:  # just proceed
                # TODO: this design choice allows the user to load partial results (their responsibility);
                #  another option is to abort this job if status is "error" or "canceled".
                pass
        else:  # assume it points to a file (convenience, non-public API)
            pass
        return None
