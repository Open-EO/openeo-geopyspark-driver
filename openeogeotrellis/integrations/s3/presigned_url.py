from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


_log = logging.getLogger(__name__)


def create_presigned_url(client: S3Client, bucket_name:str, object_name: str, expiration: int=3600) -> Optional[str]:
    """
    Generate a presigned URL to share an S3 object

    :return: Presigned URL as string. If error, returns None.
    """
    try:
        response = client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_name},
            ExpiresIn=expiration,
        )
    except ClientError as e:
        logging.info(f"Failed to create presigned url: {e}, continuing without.")
        return None

    # The response contains the presigned URL
    return response
