import pytest

from openeo_driver.errors import OpenEOApiException

from openeogeotrellis.integrations.credit_check import (
    AlwaysAllowCreditCheck,
    CreditCheck,
    ExecutionDetails,
    JOB_OPTION_CREDIT_PLANS,
)


class TestAlwaysAllowCreditCheckGetBatchExecutionDetails:
    def setup_method(self):
        self.credit_check = AlwaysAllowCreditCheck()

    def test_default_plan_when_no_job_options(self):
        result = self.credit_check.get_batch_execution_details({})
        assert result == ExecutionDetails(plan="default")

    def test_default_plan_when_job_options_is_none(self):
        result = self.credit_check.get_batch_execution_details({"job_options": None})
        assert result == ExecutionDetails(plan="default")

    def test_default_plan_when_credit_plans_key_absent(self):
        result = self.credit_check.get_batch_execution_details({"job_options": {"driver-memory": "4G"}})
        assert result == ExecutionDetails(plan="default")

    def test_uses_first_plan_from_list(self):
        result = self.credit_check.get_batch_execution_details(
            {"job_options": {JOB_OPTION_CREDIT_PLANS: ["plan-a", "plan-b"]}}
        )
        assert result == ExecutionDetails(plan="plan-a")

    def test_single_plan_used(self):
        result = self.credit_check.get_batch_execution_details({"job_options": {JOB_OPTION_CREDIT_PLANS: ["premium"]}})
        assert result == ExecutionDetails(plan="premium")


class TestCreditCheckFormatValidation:
    def setup_method(self):
        self.credit_check = AlwaysAllowCreditCheck()

    def test_valid_list_passes(self):
        # Should not raise
        self.credit_check.check_format_user_provided_plans(["plan-a", "plan-b"])

    def test_empty_list_raises(self):
        with pytest.raises(OpenEOApiException) as exc_info:
            self.credit_check.check_format_user_provided_plans([])
        assert exc_info.value.status_code == 400
        assert exc_info.value.code == "CreditPlansInvalid"

    @pytest.mark.parametrize("invalid_input", ["plan-a", 123, {"plan": "x"}, None])
    def test_non_list_raises(self, invalid_input):
        with pytest.raises(OpenEOApiException) as exc_info:
            self.credit_check.check_format_user_provided_plans(invalid_input)
        assert exc_info.value.status_code == 400
        assert exc_info.value.code == "CreditPlansInvalid"

    def test_error_message_contains_job_option_name(self):
        with pytest.raises(OpenEOApiException) as exc_info:
            self.credit_check.check_format_user_provided_plans([])
        assert JOB_OPTION_CREDIT_PLANS in exc_info.value.message


class TestGetUserProvidedCreditPlans:
    def setup_method(self):
        self.credit_check = AlwaysAllowCreditCheck()

    def test_returns_none_when_job_options_absent(self):
        assert self.credit_check.get_user_provided_credit_plans({}) is None

    def test_returns_none_when_job_options_is_none(self):
        assert self.credit_check.get_user_provided_credit_plans({"job_options": None}) is None

    def test_returns_none_when_credit_plans_key_absent(self):
        assert self.credit_check.get_user_provided_credit_plans({"job_options": {"driver-memory": "4G"}}) is None

    def test_returns_plans_when_present(self):
        result = self.credit_check.get_user_provided_credit_plans(
            {"job_options": {JOB_OPTION_CREDIT_PLANS: ["plan-a", "plan-b"]}}
        )
        assert result == ["plan-a", "plan-b"]


class TestCreditCheckAbstract:
    def test_custom_subclass_can_deny(self):
        class NeverAllowCreditCheck(CreditCheck):
            def get_batch_execution_details(self, job_details):
                self._raise_payment_required()

            def _get_message_insufficient_credits(self) -> str:
                return "No credits available. Please top up."

        credit_check = NeverAllowCreditCheck()
        with pytest.raises(OpenEOApiException) as exc_info:
            credit_check.get_batch_execution_details({"job_options": {}})
        assert exc_info.value.status_code == 402
        assert exc_info.value.code == "PaymentRequired"
