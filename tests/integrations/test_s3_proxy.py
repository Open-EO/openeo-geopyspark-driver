from openeogeotrellis.integrations.s3proxy.asset_urls import PresignedS3AssetUrls


def test_get_bucket_key_from_uri():
    bucket, key = PresignedS3AssetUrls.get_bucket_key_from_uri("s3://my-bucket/my/key")

    assert bucket == "my-bucket"
    assert key == "my/key"
