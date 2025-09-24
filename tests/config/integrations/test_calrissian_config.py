from openeogeotrellis.config.integrations.calrissian_config import DEFAULT_SECURITY_CONTEXT, CalrissianConfig


def test_calrissian_config_defaults():
    config = CalrissianConfig()
    assert config.namespace == "calrissian-demo-project"
    assert config.s3_bucket == "calrissian"
    assert "calrissian:" in config.calrissian_image
    assert "alpine:" in config.input_staging_image
    assert config.security_context is not DEFAULT_SECURITY_CONTEXT
    assert config.security_context == DEFAULT_SECURITY_CONTEXT


def test_calrissian_config_custom():
    config = CalrissianConfig(
        namespace="namezpace",
        calrissian_image="my-calrissian-image",
        input_staging_image="windoows:95",
        security_context={"run_as_user": 2000},
    )
    assert config.namespace == "namezpace"
    assert config.calrissian_image == "my-calrissian-image"
    assert config.input_staging_image == "windoows:95"
    assert config.security_context == {"run_as_user": 2000}
