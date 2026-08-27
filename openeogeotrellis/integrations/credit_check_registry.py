from typing import Dict

from openeo_driver.jobregistry import JobDict

from openeogeotrellis.integrations.credit_check import (
    CreditCheck,
    ExecutionDetails,
    AlwaysAllowCreditCheck,
)
from openeogeotrellis.config import get_backend_config


_credit_checks: Dict[str, CreditCheck] = {}


def register_credit_check(name: str, credit_check: CreditCheck) -> None:
    """Register a CreditCheck instance under the given name.

    The instance is shared (singleton) — implementations must be stateless or
    thread-safe, because the same object is reused across all requests.
    """
    assert name not in _credit_checks, "Overwriting credit checks is not allowed"
    _credit_checks[name] = credit_check


def get_credit_check() -> CreditCheck:
    name = get_backend_config().credit_check_name
    if name not in _credit_checks:
        raise KeyError(f"No credit check registered under name {name!r}. ")
    return _credit_checks[name]


def get_batch_execution_details(job_details: JobDict) -> ExecutionDetails:
    return get_credit_check().get_batch_execution_details(job_details)


register_credit_check("AlwaysAllowCreditCheck", AlwaysAllowCreditCheck())
