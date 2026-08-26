import pytest
from unittest.mock import MagicMock

from openeo_driver.errors import OpenEOApiException

import openeogeotrellis.integrations.credit_check as credit_check_module
from openeogeotrellis.integrations.credit_check import (
    AlwaysAllowCreditCheck,
    CreditCheck,
    ExecutionDetails,
    JOB_OPTION_CREDIT_PLANS,
    get_batch_execution_details,
    get_credit_check,
    register_credit_check,
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

    @pytest.mark.parametrize("invalid_input", ["plan-a", 123, {"plan": "x"}, None])
    def test_non_list_raises(self, invalid_input):
        with pytest.raises(OpenEOApiException) as exc_info:
            self.credit_check.check_format_user_provided_plans(invalid_input)
        assert exc_info.value.status_code == 400
        assert exc_info.value.code == "CreditPlansInvalid"

    def test_error_message_contains_job_option_name(self):
        with pytest.raises(OpenEOApiException) as exc_info:
            self.credit_check.check_format_user_provided_plans("invalidInput")
        assert JOB_OPTION_CREDIT_PLANS in exc_info.value.message


class TestGetUserProvidedCreditPlans:
    def setup_method(self):
        self.credit_check = AlwaysAllowCreditCheck()

    def test_returns_empty_list_when_job_options_absent(self):
        assert self.credit_check.get_user_provided_credit_plans({}) == []

    def test_returns_empty_list_when_job_options_is_none(self):
        assert self.credit_check.get_user_provided_credit_plans({"job_options": None}) == []

    def test_returns_empty_list_when_credit_plans_key_absent(self):
        assert self.credit_check.get_user_provided_credit_plans({"job_options": {"driver-memory": "4G"}}) == []

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


@pytest.fixture
def isolated_registry(monkeypatch):
    """Replace the module-level registry with a fresh dict for each test."""
    fresh = {}
    monkeypatch.setattr(credit_check_module, "_credit_checks", fresh)
    return fresh


@pytest.fixture
def mock_config(monkeypatch):
    """Return a helper that sets the active credit_check name in config."""
    config = MagicMock()
    monkeypatch.setattr(credit_check_module, "get_backend_config", lambda: config)
    return config


class TestRegisterCreditCheck:
    def test_registers_instance_under_name(self, isolated_registry):
        instance = AlwaysAllowCreditCheck()
        register_credit_check("MyCheck", instance)
        assert isolated_registry["MyCheck"] is instance

    def test_rejects_duplicate_name(self, isolated_registry):
        register_credit_check("MyCheck", AlwaysAllowCreditCheck())
        with pytest.raises(AssertionError, match="Overwriting credit checks is not allowed"):
            register_credit_check("MyCheck", AlwaysAllowCreditCheck())

    def test_allows_different_names(self, isolated_registry):
        register_credit_check("CheckA", AlwaysAllowCreditCheck())
        register_credit_check("CheckB", AlwaysAllowCreditCheck())
        assert "CheckA" in isolated_registry
        assert "CheckB" in isolated_registry


class TestGetCreditCheck:
    def test_returns_registered_instance(self, isolated_registry, mock_config):
        instance = AlwaysAllowCreditCheck()
        isolated_registry["MyCheck"] = instance
        mock_config.credit_check = "MyCheck"
        assert get_credit_check() is instance

    def test_raises_for_unregistered_name(self, isolated_registry, mock_config):
        mock_config.credit_check = "UnknownCheck"
        with pytest.raises(KeyError, match="UnknownCheck"):
            get_credit_check()


class TestGetBatchExecutionDetails:
    def test_delegates_to_registered_implementation(self, isolated_registry, mock_config):
        isolated_registry["AlwaysAllowCreditCheck"] = AlwaysAllowCreditCheck()
        mock_config.credit_check = "AlwaysAllowCreditCheck"
        result = get_batch_execution_details({})
        assert result == ExecutionDetails(plan="default")

    def test_uses_plan_from_job_options(self, isolated_registry, mock_config):
        isolated_registry["AlwaysAllowCreditCheck"] = AlwaysAllowCreditCheck()
        mock_config.credit_check = "AlwaysAllowCreditCheck"
        result = get_batch_execution_details({"job_options": {JOB_OPTION_CREDIT_PLANS: ["premium"]}})
        assert result == ExecutionDetails(plan="premium")
