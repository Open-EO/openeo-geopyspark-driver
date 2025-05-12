from openeogeotrellis.integrations.s3_client import get_s3_client
import pytest

from openeogeotrellis.integrations.s3_client.credentials import get_credentials


# Without protocol because legacy eodata config did not have protocol in endpoint
eodata_test_endpoint = "eodata.oeo.test"
legacy_test_endpoint = "https://customs3.oeo.test"


@pytest.fixture
def legacy_fallback_endpoint(monkeypatch):
    monkeypatch.setenv("SWIFT_URL", legacy_test_endpoint)


@pytest.fixture
def historic_eodata_endpoint_env_config(monkeypatch):
    monkeypatch.setenv("AWS_S3_ENDPOINT", eodata_test_endpoint)
    monkeypatch.setenv("AWS_HTTPS", "NO")


@pytest.fixture
def new_eodata_endpoint_env_config(monkeypatch):
    monkeypatch.setenv("EODATA_S3_ENDPOINT", f"https://{eodata_test_endpoint}")


@pytest.fixture
def swift_credentials(monkeypatch):
    monkeypatch.setenv("SWIFT_ACCESS_KEY_ID", "swiftkey")
    monkeypatch.setenv("SWIFT_SECRET_ACCESS_KEY", "swiftsecret")


@pytest.mark.parametrize(
    "region_name,expected_endpoint",
    [
        pytest.param(
            "waw3-1",
            "https://s3.waw3-1.cloudferro.com",
        ),
        pytest.param(
            "waw3-2",
            "https://s3.waw3-2.cloudferro.com",
        ),
        pytest.param(
            "waw4-1",
            "https://s3.waw4-1.cloudferro.com",
        ),
        pytest.param("eu-nl", "https://obs.eu-nl.otc.t-systems.com"),
        pytest.param("eu-de", "https://obs.eu-de.otc.t-systems.com"),
        pytest.param("eodata", f"http://{eodata_test_endpoint}"),
        pytest.param("eu-faketest-central", legacy_test_endpoint),
    ],
)
def test_s3_client_has_expected_endpoint_and_region(
    historic_eodata_endpoint_env_config,
    swift_credentials,
    legacy_fallback_endpoint,
    region_name: str,
    expected_endpoint: str,
):
    c = get_s3_client(region_name)
    assert region_name == c.meta.region_name
    assert expected_endpoint == c.meta.endpoint_url


@pytest.mark.parametrize(
    "region_name,expected_endpoint",
    [
        pytest.param("eodata", f"https://{eodata_test_endpoint}"),
    ],
)
def test_s3_client_prefered_eodata_config(
    new_eodata_endpoint_env_config,
    historic_eodata_endpoint_env_config,
    swift_credentials,
    region_name: str,
    expected_endpoint: str,
):
    c = get_s3_client(region_name)
    assert region_name == c.meta.region_name
    assert expected_endpoint == c.meta.endpoint_url


@pytest.mark.parametrize(
    "region_name,env,exp_akid,exp_secret",
    [
        pytest.param(
            "waw3-1",
            {
                "SWIFT_ACCESS_KEY_ID": "swiftkey",
                "SWIFT_SECRET_ACCESS_KEY": "swiftsecret",
                "AWS_ACCESS_KEY_ID": "awskey",
                "AWS_SECRET_ACCESS_KEY": "awssecret",
            },
            "swiftkey",
            "swiftsecret",
            id="backwardsCompatibleSwiftFallbackButBeforeAWS",
        ),
        pytest.param(
            "waw3-1",
            {
                "AWS_ACCESS_KEY_ID": "awskey",
                "AWS_SECRET_ACCESS_KEY": "awssecret",
            },
            "awskey",
            "awssecret",
            id="backwardsCompatibleFallbackAWS",
        ),
        pytest.param(
            "waw3-1",
            {
                "OTC_ACCESS_KEY_ID": "cfaccess",
                "OTC_SECRET_ACCESS_KEY": "csecret",
                "SWIFT_ACCESS_KEY_ID": "swiftkey",
                "SWIFT_SECRET_ACCESS_KEY": "swiftsecret",
                "AWS_ACCESS_KEY_ID": "awskey",
                "AWS_SECRET_ACCESS_KEY": "awssecret",
            },
            "swiftkey",
            "swiftsecret",
            id="VendorSpecificCredentialsTakePrecedenceButIgnoredIfNotRightVendor",
        ),
        pytest.param(
            "waw3-1",
            {
                "CF_ACCESS_KEY_ID": "cfaccess",
                "CF_SECRET_ACCESS_KEY": "csecret",
                "SWIFT_ACCESS_KEY_ID": "swiftkey",
                "SWIFT_SECRET_ACCESS_KEY": "swiftsecret",
                "AWS_ACCESS_KEY_ID": "awskey",
                "AWS_SECRET_ACCESS_KEY": "awssecret",
            },
            "cfaccess",
            "csecret",
            id="VendorSpecificCredentialsTakePrecedenceIfMatch",
        ),
        pytest.param(
            "waw3-1",
            {
                "CF_WAW3_1_ACCESS_KEY_ID": "cfaccesswaw31",
                "CF_WAW3_1_SECRET_ACCESS_KEY": "csecretwaw31",
                "CF_ACCESS_KEY_ID": "cfaccess",
                "CF_SECRET_ACCESS_KEY": "csecret",
                "SWIFT_ACCESS_KEY_ID": "swiftkey",
                "SWIFT_SECRET_ACCESS_KEY": "swiftsecret",
                "AWS_ACCESS_KEY_ID": "awskey",
                "AWS_SECRET_ACCESS_KEY": "awssecret",
            },
            "cfaccesswaw31",
            "csecretwaw31",
            id="RegionSpecificCredentialsTakePrecedenceIfMatch",
        ),
        pytest.param(
            "eodata",
            {
                "CF_WAW3_1_ACCESS_KEY_ID": "cfaccesswaw31",
                "CF_WAW3_1_SECRET_ACCESS_KEY": "csecretwaw31",
                "CF_ACCESS_KEY_ID": "cfaccess",
                "CF_SECRET_ACCESS_KEY": "csecret",
                "EODATA_ACCESS_KEY_ID": "eodataaccess",
                "EODATA_SECRET_ACCESS_KEY": "eodataecret",
                "SWIFT_ACCESS_KEY_ID": "swiftkey",
                "SWIFT_SECRET_ACCESS_KEY": "swiftsecret",
                "AWS_ACCESS_KEY_ID": "awskey",
                "AWS_SECRET_ACCESS_KEY": "awssecret",
            },
            "eodataaccess",
            "eodataecret",
            id="EodataProviderConfigIsResolvable",
        ),
    ],
)
def test_s3_credentials_retrieval_from_env(monkeypatch, region_name: str, env: dict, exp_akid, exp_secret: str):
    for env_var, env_val in env.items():
        monkeypatch.setenv(env_var, env_val)
    creds = get_credentials(region_name)
    assert exp_akid == creds["aws_access_key_id"]
    assert exp_secret == creds["aws_secret_access_key"]


def test_exception_when_not_having_legacy_config_and_unsupported_region(
    historic_eodata_endpoint_env_config, swift_credentials
):
    with pytest.raises(EnvironmentError):
        get_s3_client("eu-faketest-central")
