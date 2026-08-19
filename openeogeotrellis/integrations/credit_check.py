from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List

from openeo_driver.errors import OpenEOApiException
from openeo_driver.jobregistry import JobDict

JOB_OPTION_CREDIT_PLANS = "credit-plans"


@dataclass
class ExecutionDetails:
    # The chosen plan based on user preference + credit availability determined at start time
    plan: str
    # Optional another backend url in case the job should run elsewhere: TODO implement in the future
    backend_url: Optional[str] = None


class CreditCheck(ABC):
    """
    An abstract class that should be extended by code performing credit checks. The abstract methods have their own
    documentation on what they should do. Methods starting with `_hook_` allow for customised messaging and improved
    customer experience.
    """

    @abstractmethod
    def get_batch_execution_details(self, job_details: JobDict) -> ExecutionDetails:
        """
        This method gets the current job details and will decide whether execution is allowed or not.

        If not allowed it should call the _raise_payment_required method

        """
        raise NotImplementedError()

    @abstractmethod
    def _get_message_insufficient_credits(self) -> str:
        """
        Implementation should return a custom message for what to do if credits run out. For example instructions for
        users on how to top up their credits.
        """
        raise NotImplementedError()

    def get_job_option_description(self) -> str:
        """
        This can be overriden because implementations have more information (e.g. supported plans)
        """
        return "A list of the credit-types to be used. The first credit type for which credits are available is used."

    def _raise_payment_required(self) -> None:
        raise OpenEOApiException(
            code="PaymentRequired",
            message=self._get_message_insufficient_credits(),
            status_code=402,  # PaymentRequired
        )

    def _hook_get_message_invalid_plan(self, details) -> str:
        return f"The provided job-option {JOB_OPTION_CREDIT_PLANS} is invalid. {details}"

    def _raise_invalid_plan(self, err_details: str = "") -> None:
        # https://github.com/Open-EO/openeo-api/blob/1881dae18b3c2c417f1305774cf295c81d60d400/errors.json#L332
        raise OpenEOApiException(
            code="CreditPlansInvalid",
            message=self._hook_get_message_invalid_plan(err_details),
            status_code=400,
        )

    def check_format_user_provided_plans(self, user_provided_plans: List[str]):
        """
        This is used to verify the job-option value that is provided by the user.

        It receives a list of plans as provided by the user and returns an OpenEO Exception if not invalid.

        credit-plans supports a list of plans to allow specifying a preference of credit type consumption\

        The backend must call self._raise_invalid_plan even if there are invalid plans in the list even when some are
        valid. Implementation classes should call this super method first and then perform their additional checks.
        """
        if isinstance(user_provided_plans, list):
            for plan in user_provided_plans:
                if not isinstance(plan, str):
                    self._raise_invalid_plan(f"Plan should be a list of strings got a list containing {type(plan)}")
        else:
            self._raise_invalid_plan(f"Plan should be a list of strings got a {type(user_provided_plans)}")

    def get_user_provided_credit_plans(self, job_details: JobDict) -> Optional[List[str]]:
        """
        Get the credit plans that were provided by the user and have been validated as being valid job-options.

        If the user did not specify a value this will return None
        """
        job_options_dict = job_details.get("job_options", {}) or {}
        return job_options_dict.get(JOB_OPTION_CREDIT_PLANS)


class AlwaysAllowCreditCheck(CreditCheck):
    """
    A dummy implementation that always assumes credits are available and that the provided plans have valid names.
    If no plans are provided it will select a default plan.
    """

    @staticmethod
    def _get_default_plans() -> List[str]:
        return ["default"]

    def get_batch_execution_details(self, job_details: JobDict) -> ExecutionDetails:
        plans_from_job_options = self.get_user_provided_credit_plans(job_details) or self._get_default_plans()

        chosen_plan = plans_from_job_options[0]
        return ExecutionDetails(chosen_plan)

    def _get_message_insufficient_credits(self) -> str:
        # https://github.com/Open-EO/openeo-api/blob/1881dae18b3c2c417f1305774cf295c81d60d400/errors.json#L325C15-L325C104
        return "The budget required to fulfil the request is not sufficient. A payment is required first."
