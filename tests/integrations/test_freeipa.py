import textwrap
from typing import Optional, List

import dirty_equals
import pytest

from openeogeotrellis.integrations.freeipa import FreeIpaClient, get_freeipa_server_from_env, get_verify_tls_from_env


class TestFreeIpaClient:
    def _get_ipa_handler(
        self,
        expected_method: str,
        expected_arguments: Optional[List[str]] = None,
        expected_options: Optional[dict] = None,
        result: Optional[dict] = None,
        error: Optional[dict] = None,
    ):
        """Helper to create a requests_mock handler for FreeIPA API calls"""

        def handle(request, context):
            request_data = request.json()
            assert request_data == {
                "id": dirty_equals.IsStr(),
                "method": expected_method,
                "params": [expected_arguments or [], {**(expected_options or {}), "version": dirty_equals.IsStr()}],
            }
            return {
                "id": request_data["id"],
                "error": error,
                "result": result,
            }

        return handle

    @pytest.mark.parametrize(
        "ipa_server",
        [
            "ipa.test",
            "https://ipa.test",
            "http://ipa.test",
        ],
    )
    def test_ipa_server_normalization(self, requests_mock, ipa_server):
        def handle(request, context):
            return {"id": request.json()["id"], "result": {"result": {"name": "John Doe"}}}

        requests_mock.post("http://ipa.test/ipa/json", json=handle)
        requests_mock.post("https://ipa.test/ipa/json", json=handle)
        client = FreeIpaClient(ipa_server=ipa_server, verify_tls=False)
        user_data = client.user_show("john")
        assert user_data == {"name": "John Doe"}

    def test_user_show(self, requests_mock):
        requests_mock.post(
            "https://ipa.test/ipa/json",
            json=self._get_ipa_handler(
                expected_method="user_show",
                expected_arguments=["john"],
                result={"result": {"givenname": "John", "sn": "Doe"}},
            ),
        )
        client = FreeIpaClient(ipa_server="ipa.test", verify_tls=False)
        user_data = client.user_show("john")
        assert user_data == {"givenname": "John", "sn": "Doe"}

    def test_user_find(self, requests_mock):
        requests_mock.post(
            "https://ipa.test/ipa/json",
            json=self._get_ipa_handler(
                expected_method="user_find",
                expected_options={"uid": "john", "sizelimit": 2},
                result={"result": [{"givenname": "John", "sn": "Doe"}]},
            ),
        )
        client = FreeIpaClient(ipa_server="ipa.test", verify_tls=False)
        user_data = client.user_find(uid="john")
        assert user_data == {"givenname": "John", "sn": "Doe"}

    def test_user_add(self, requests_mock):
        requests_mock.post(
            "https://ipa.test/ipa/json",
            json=self._get_ipa_handler(
                expected_method="user_add",
                expected_arguments=["john"],
                expected_options={"givenname": "John", "sn": "Doe", "mail": "john@example.com"},
                result={
                    "result": {
                        "uid": ["john"],
                        "uidnumber": ["631722026"],
                        "givenname": ["John"],
                        "cn": ["John Doe"],
                        "displayname": ["John Doe"],
                        "initials": ["JD"],
                        "sn": ["Doe"],
                        "mail": ["john@example.com"],
                    },
                    "value": "john",
                },
            ),
        )
        client = FreeIpaClient(ipa_server="ipa.test", verify_tls=False)
        user_data = client.user_add(uid="john", first_name="John", last_name="Doe", email="john@example.com")
        assert user_data == dirty_equals.IsPartialDict(givenname=["John"], sn=["Doe"], mail=["john@example.com"])

    def test_user_add_with_additional_options(self, requests_mock):
        requests_mock.post(
            "https://ipa.test/ipa/json",
            json=self._get_ipa_handler(
                expected_method="user_add",
                expected_arguments=["john"],
                expected_options={"givenname": "John", "sn": "Doe", "mail": "john@example.com", "userorigin": "mars"},
                result={
                    "result": {
                        "uid": ["john"],
                        "uidnumber": ["631722026"],
                        "givenname": ["John"],
                        "cn": ["John Doe"],
                        "displayname": ["John Doe"],
                        "initials": ["JD"],
                        "userorigin": ["mars"],
                        "sn": ["Doe"],
                        "mail": ["john@example.com"],
                    },
                    "value": "john",
                },
            ),
        )
        client = FreeIpaClient(ipa_server="ipa.test", verify_tls=False)
        user_data = client.user_add(
            uid="john",
            first_name="John",
            last_name="Doe",
            email="john@example.com",
            additional_options={"userorigin": "mars"},
        )
        assert user_data == dirty_equals.IsPartialDict(
            givenname=["John"], sn=["Doe"], mail=["john@example.com"], userorigin=["mars"]
        )


def test_guess_freeipa_server(tmp_path, monkeypatch):
    path = tmp_path / "ipa-default.conf"
    path.write_text(
        textwrap.dedent(
            """
        # ipa config

        [global]
        realm = EXAMPLE.COM
        domain = cloud.example.com
        server = ipa01.example.com
        enable_ra = True
        """
        )
    )

    assert get_freeipa_server_from_env(conf=path) == "ipa01.example.com"

    monkeypatch.setenv("OPENEO_FREEIPA_SERVER", "ipa02.test")
    assert get_freeipa_server_from_env(conf=path) == "ipa02.test"


def test_guess_verify_tls(monkeypatch):
    # No/empty env var
    assert get_verify_tls_from_env() is True
    monkeypatch.setenv("OPENEO_FREEIPA_VERIFY_TLS", "")
    assert get_verify_tls_from_env() is True

    # Falsy
    for value in ["False", "no", "0"]:
        monkeypatch.setenv("OPENEO_FREEIPA_VERIFY_TLS", value)
        assert get_verify_tls_from_env() is False

    # Truthy
    for value in ["True", "oh yeah", "1"]:
        monkeypatch.setenv("OPENEO_FREEIPA_VERIFY_TLS", value)
        assert get_verify_tls_from_env() is True

    # Custom CA certificate file
    monkeypatch.setenv("OPENEO_FREEIPA_VERIFY_TLS", "/etc/ipa/ca.crt")
    assert get_verify_tls_from_env() == "/etc/ipa/ca.crt"
