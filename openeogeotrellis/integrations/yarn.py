import logging

from openeo_driver.jobregistry import JOB_STATUS

_log = logging.getLogger(__name__)


class YARN_STATE:
    """
    Enumeration of various states of a YARN ApplicationMaster

    Also see https://hadoop.apache.org/docs/r3.1.1/api/org/apache/hadoop/yarn/api/records/YarnApplicationState.html
    """
    ACCEPTED = "ACCEPTED"
    FAILED = "FAILED"
    FINISHED = "FINISHED"
    KILLED = "KILLED"
    NEW = "NEW"
    NEW_SAVING = "NEW_SAVING"
    RUNNING = "RUNNING"
    SUBMITTED = "SUBMITTED"


class YARN_FINAL_STATUS:
    """
    Enumeration of various final states of a YARN Application

    Also see https://hadoop.apache.org/docs/r3.1.1/api/org/apache/hadoop/yarn/api/records/FinalApplicationStatus.html
    """
    ENDED = "ENDED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    SUCCEEDED = "SUCCEEDED"
    UNDEFINED = "UNDEFINED"


def yarn_state_to_openeo_job_status(state: str, final_state: str) -> str:
    """Map YARN app state to openEO batch job status"""
    if state in {YARN_STATE.NEW, YARN_STATE.SUBMITTED, YARN_STATE.ACCEPTED}:
        # Note: once app is in YARN (the user triggered start of batch job)
        # the batch job status should be at least "queued" ("created" is reserved for pre-start phase).
        job_status = JOB_STATUS.QUEUED
    elif state == YARN_STATE.RUNNING:
        job_status = JOB_STATUS.RUNNING
    elif state == YARN_STATE.KILLED or final_state == YARN_FINAL_STATUS.KILLED:
        job_status = JOB_STATUS.CANCELED
    elif final_state == YARN_FINAL_STATUS.SUCCEEDED:
        job_status = JOB_STATUS.FINISHED
    elif final_state == YARN_STATE.FAILED:
        job_status = JOB_STATUS.ERROR
    else:
        _log.warning(f"Unhandled YARN app state mapping: {(state, final_state)}")
        # Fallback to minimal status "queued" (once in YARN, batch job status should be at least "queued")
        job_status = JOB_STATUS.QUEUED

    return job_status
