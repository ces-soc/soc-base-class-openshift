"""
The s3 package provides standard s3 functionality for cessoc services.
"""
import os
from typing import Optional
import boto3
from botocore.exceptions import ClientError
from cessoc.logging import cessoc_logging


def write(
    key: str,
    body: str,
    bucket: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region_name: Optional[str] = "us-west-2",
) -> None:
    """
    S3 PUT operator for base class

    :param key: the path + name of file to be accessed
    :param bucket: the bucket containing the file
    :param body: body of file to be written
    :param access_key: AWS access key id for bucket call
    :param secret_key: AWS secret key matching access key
    :param region_name: Region name of bucket (if needed)

    :raises ClientError: on boto3 python sdk error
    """
    logger = cessoc_logging.getLogger("cessoc")
    if bucket is None:
        bucket = f"ces-soc-etl-{os.getenv('STAGE')}-{os.getenv('CAMPUS')}"
    logger.debug("Putting data to %s in s3 bucket %s", key, bucket)
    if access_key and secret_key:
        client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name
        )
    else:
        client = boto3.client("s3", region_name=region_name)
    try:
        response = client.put_object(Key=key, Bucket=bucket, Body=body)
        logger.debug(response)
    except ClientError as ex:
        logger.error("Could not put to s3: %s", ex)
        raise ex


def read(
    key: str,
    bucket: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region_name: Optional[str] = "us-west-2",
) -> bytes:
    """
    S3 GET operator for base class

    :param key: the path + name of file to be accessed
    :param bucket: the bucket containing the file
    :param access_key: AWS access key id for bucket call
    :param secret_key: AWS secret key matching access key
    :param region_name: Region name of bucket (if needed)

    :raises ClientError: on boto3 python sdk error

    :returns: Bucket data as string or as bytes.
    """
    logger = cessoc_logging.getLogger("cessoc")
    if bucket is None:
        bucket = f"ces-soc-etl-{os.getenv('STAGE')}-{os.getenv('CAMPUS')}"
    logger.debug("Accessing %s in s3 bucket %s", key, bucket)
    if access_key and secret_key:
        client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
        )
    else:
        client = boto3.client("s3", region_name=region_name)
    try:
        response = client.get_object(Key=key, Bucket=bucket)
        logger.debug(response)
    except ClientError as ex:
        logger.error("Could not get from S3: %s", ex)
        raise ex
    else:
        return response["Body"].read().decode("utf-8")
