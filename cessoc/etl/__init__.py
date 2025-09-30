"""
ETL Base Class

Provides various utilities for ease of use in building ETL services.
Use the etl-template repository for a starting point
"""


import json
import logging
import os
import time
import ssl
import uuid
from abc import ABC, abstractmethod
from typing import Dict, List, Union, Optional
from datetime import datetime, timedelta

import pika
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import boto3
from botocore.exceptions import ClientError

from cessoc.configmanager import ConfigManager


class ETL(ABC):
    """
    Abstract class for non-event-driven microservices (i.e. services not using MQ.
    Use this in conjunction with etl template class to build your ETL microservice
    """

    def __init__(self):
        """Provides a config dictionary for derived class"""
        self.config_manager = ConfigManager()
        self.config = None
        self.logger = logging.getLogger(
            self.__class__.__name__
        )  # This line should come after the config manager is intialized because the config_manager configures the root logger

    def _send_humio(
        self,
        chunked_data: List,
        endpoint: str = None,
        token: str = None,
        healthcheck: bool = False,
    ):
        r"""
        Initialize Elastic Client for connection to Humio
        Each chunk of data should be formatted as such:
        [
            {
                "message": [
                    "{\"event1\": \"the entire event must be JSON encoded string\"}",
                    "{\"event2\": 1234}
                ]
            }
        ]

        :param chunked_data: A list of chunks of event data, formatted according to Humio Ingest API unstructured ingest
        :param endpoint: On-prem or remote endpoint for humio data exports
        :param token: Humio-generated token for data ingress
        :param healthcheck: Boolean to inform the client that it is creating a healthcheck connection

        :raises KeyError: if CAMPUS variable is not set
        :raises Exception: if the configuration is missing for humio connections
        :raises ValueError: if the endpoint or token is 'None' when healthcheck is False
        :raises HTTPError: if any POST request to Humio errors
        """
        try:
            try:
                self.campus_name = os.environ["CAMPUS"]
            except KeyError as ex:
                raise KeyError("CAMPUS environment variable is undefined") from ex
            if healthcheck:
                # Creates a specific connection using healthcheck credentials to humio
                if "ON_PREM_DEPLOY" in os.environ:
                    self.logger.debug("Accessing ON-PREM endpoint, credentials")
                    humio_endpoint = self.get_value(
                        f"/{self.campus_name}/secops-humio/config/ingest_api-on_prem"
                    )
                    humio_token = self.get_value(
                        f"/{self.campus_name}/secops-humio/secrets/healthcheck/ingest_token"
                    )
                else:
                    self.logger.debug("Accessing ECS healthcheck endpoint, credentials")
                    humio_endpoint = self.get_value(
                        f"/{self.campus_name}/secops-humio/config/ingest_api"
                    )
                    humio_token = self.get_value(
                        f"/{self.campus_name}/secops-humio/secrets/healthcheck/ingest_token"
                    )
            else:
                if not endpoint:
                    raise ValueError("endpoint must be provided if not a health check")
                if not token:
                    raise ValueError("token must be provided if not a health check")
                # Otherwise create a connection using credentials from child class
                humio_endpoint = endpoint
                humio_token = token
            # Create a HTTP session
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                # These status codes indicate something temporarily wrong, fixable by re-request
                status_forcelist=[408, 429, 500, 502, 503, 504],
                allowed_methods=["GET", "POST"],
            )
            session = requests.Session()
            session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

            # Make the ingest POSTs in chunks
            for data in chunked_data:
                resp = session.post(
                    humio_endpoint,
                    json=data,
                    headers={"Authorization": "Bearer " + humio_token},
                )
                resp.raise_for_status()
                if not healthcheck:
                    self.logger.info(
                        f"Event batch of size {len(data[0]['messages'])} has been sent to Humio"
                    )

        except ClientError as ex:
            raise Exception("Unable to get humio endpoint/ingest_token") from ex

    def _healthcheck(
        self, data: Union[Dict, List, str], destination: Optional[str], export_type: str
    ) -> None:
        """
        Reports statistics on intel to Humio
        Requires Humio custom parser:
        parseJson() | parseTimestamp(format=unixTimeSeconds, field=@timestamp)

        :param data: Data in a list or json format
        :param destination: The location where data is being written to (e.g. the bucket + file name)
        :param export_type: The type of export (Humio or S3)

        :raises Exception: if Client Configuration is unavailable or missing
        """
        path = "service-health"
        health_obj = {
            "@timestamp": round(time.time()),
            "version": "v0.0.1",
            "destination": destination,
            "export_type": export_type,
            "data_count": len(data),
            "data_type": str(type(data)),
            "service": self.__class__.__name__,
        }
        try:
            self.write_humio([health_obj], path=path, healthcheck=True)
            self.logger.debug("Healthcheck event: %s", json.dumps(health_obj))
        except ClientError as ex:
            raise Exception("Client Configuration unavailable or missing") from ex

    def publish_message_campus(
        self, routing_key: str, body: Dict, reply_to: bool = False, timeout: int = 300
    ) -> Optional[Dict]:
        """
        Sends a message on the eventhub to this campus exchange on the routing key
        :param routing_key: The routing key to send the message to
        :param body: The body of the message to send
        :param reply_to: if an reply is expected (blocking until reply is received)
        :param timeout: The timeout to wait for a response in seconds
        :return: The dict of the reply if requested
        """
        return self.publish_message(
            self.config["campus_name"].lower(), routing_key, body, reply_to, timeout
        )

    def publish_message(
        self,
        exchange: str,
        routing_key: str,
        body: Dict,
        reply_to: bool = False,
        timeout: int = 300,
    ) -> Optional[Dict]:
        """
        Sends a message on the eventhub to the exchange on the routing key
        :param exchange: The exchange to publish to
        :param routing_key: The routing key to send the message to
        :param body: The body of the message to send
        :param reply_to: if an reply is expected (blocking until reply is received)
        :param timeout: The timeout to wait for a response in seconds
        :return: The dict of the reply if requested
        """
        # Create connection
        json_credentials = self.config["parameters"]["eventhub/secrets/edm/credentials"]
        endpoint = self.config["parameters"]["eventhub/config/mq_endpoint"]
        credentials = pika.PlainCredentials(
            json_credentials["username"], json_credentials["password"]
        )
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                endpoint,
                credentials=credentials,
                ssl_options=pika.SSLOptions(context=ssl.create_default_context()),
                client_properties={"connection_name": self.__class__.__name__},
            )
        )

        # Create channel
        channel = connection.channel()
        channel.exchange_declare(
            exchange, exchange_type="direct", passive=True, durable=True
        )

        # Setup the replyto queue
        reply_to_queue = None
        if reply_to:
            reply_to_queue = f"replyto.{self.__class__.__name__}"
            channel.queue_declare(reply_to_queue, exclusive=True, auto_delete=True)

        # Setup some metadata
        correlation_id = uuid.uuid4().hex
        headers: Optional[Dict] = None
        if reply_to:
            if headers is None:
                headers = {}
            headers[
                "Reply-To-Callback"
            ] = "inline_blocking"  # Removing this errors out the replying EDM

        properties = pika.BasicProperties(
            app_id=self.__class__.__name__,
            user_id="edm",
            content_type="application/json",
            content_encoding="utf-8",
            reply_to=reply_to_queue,
            correlation_id=correlation_id,
            priority=None,
            headers=headers,
        )

        # Publish the message
        try:
            channel.basic_publish(
                exchange, routing_key, json.dumps(body), properties, mandatory=True
            )
            self.logger.info(
                "Published message to exchange '%s' with routing key '%s' and correlation id '%s'",
                exchange,
                routing_key,
                correlation_id,
            )
        except pika.exceptions.UnroutableError:
            self.logger.error("Message was unroutable")

        # Consume all messages on the response queue
        if reply_to and reply_to_queue is not None:
            start_time = int(time.time())
            while start_time + timeout > int(
                time.time()
            ):  # Weird looping to accommodate timeout with a generator
                method_frame, properties, reply_body = channel.basic_get(reply_to_queue)
                if (
                    method_frame is not None
                    and properties is not None
                    and reply_body is not None
                ):
                    if (
                        properties.correlation_id == correlation_id
                    ):  # Only accept the correlated reply
                        self.logger.info(
                            "Reply received with correlation id: %s", correlation_id
                        )
                        channel.basic_ack(method_frame.delivery_tag)
                        channel.cancel()  # Re-queues anything it may have picked up
                        channel.close()
                        connection.close()
                        return json.loads(reply_body)
                    else:  # Reject non-correlated messages
                        self.logger.debug("Rejecting message")
                        channel.basic_nack(method_frame.delivery_tag)
                    channel.cancel()
                    break
            self.logger.error(
                "Message timed out before a reply was received, correlation id: %s",
                correlation_id,
            )

        # Clean up connection and channel
        channel.close()
        connection.close()
        return None

    def slice_timestamp_range(
        self,
        last_request_time_param: str,
        current_time_param: str,
        chunk_size_in_hours: int = 1,
    ) -> List[Dict[str, str]]:
        """
        Takes in the last requested time and compares it to the current time, then breaks
        it down into chunks according to the chunk_size_in_hours parameter
        :param last_request_time_param: Last timestamp that was requested
        :param current_time_param: The current time, represented just as a string
        :param chunk_size_in_hours: Size of time chunks, represented in number of hours
        :return: A list of dictionaries containing a start_time and end_time
        NOTE: All timestamps must be in UTC
        In order to comply with the API requirements:
        - endTime must not be before startTime
        - startTime must not be beyond 7 days in the past
        - startTime and endTime must be up to or equal to 24 hours from each other
        this function will chunk the timestamps into an array of dicts:
        time_ranges = [
            {
                "start_time": "%Y-%m-%dT%H:%M:%S",
                "end_time": "%Y-%m-%dT%H:%M:%S"
            }
        ]
        """
        time_ranges: List[Dict[str, str]] = []
        last_request_time = datetime.fromisoformat(last_request_time_param).replace(
            tzinfo=None
        )
        current_time = datetime.fromisoformat(current_time_param).replace(tzinfo=None)

        # endTime must not be before startTime
        if current_time < last_request_time:
            return time_ranges

        # Start time cannot be more than 7 days in past (1 minute leeway given)
        if last_request_time < (current_time - timedelta(days=6, hours=23, minutes=59)):
            self.logger.info(
                "Last request timestamp exceeds 7 days. Resetting timestamp to 7 days ago"
            )
            last_request_time = current_time - timedelta(days=6, hours=23, minutes=59)

        # Start time and end time cannot be more than 24 hours apart
        # Because of high-event amounts, timestamps are chunked by chunk_size_in_hours
        if last_request_time < (current_time - timedelta(hours=chunk_size_in_hours)):
            self.logger.debug(
                "Start time and end time interval exceeds %s hours", chunk_size_in_hours
            )
            while True:
                next_time = last_request_time + timedelta(hours=chunk_size_in_hours)
                if next_time < current_time:  # Current time and last_request_time
                    time_range = {
                        "start_time": last_request_time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "end_time": next_time.strftime("%Y-%m-%dT%H:%M:%S"),
                    }
                    time_ranges.append(time_range)
                    last_request_time = next_time
                else:  # Timestamp is now within chunk_size_in_hours parameter of current time
                    time_range = {
                        "start_time": last_request_time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "end_time": str(current_time),
                    }
                    time_ranges.append(time_range)
                    break
        else:
            time_range = {
                "start_time": str(last_request_time),
                "end_time": str(current_time),
            }
            time_ranges.append(time_range)
        self.logger.debug(time_ranges)
        return time_ranges

    def write_s3(
        self,
        data: Union[List, Dict, str, bytes],
        etl_destination: Optional[str] = None,
        intel_file: Optional[str] = None,
    ) -> None:
        """
        Writes data to an object to etl_destination (config.json) S3 bucket in the path specified by intel_file (config.json)
        Data must be a dictionary

        :param data: A collection of data objects to write to S3
        :param etl_destination: The S3 bucket which will receive the data
        :param intel_file: The S3 bucket key or file path with file name

        :raises TypeError: if the data is not of type list, dict, or str
        :raises KeyError: if the config.json does not include the required fields
        :raises ClientError: in the case where writing to the bucket failed (e.g. Access Denied, No Such Bucket)
        :raises Exception: if for any other reason the write to S3 fails
        """
        if not isinstance(data, (list, dict, str, bytes)):
            raise TypeError(
                f"Data to write to S3 must be of type 'list', 'dict', 'str', or 'bytes' not '{type(data)}'"
            )

        try:
            # Check for function parameter overrides for bucket name
            bucket_name = (
                self.config["etl_destination"]
                if etl_destination is None
                else etl_destination
            )
            # Check for function parameter overrides for file path + filename
            file_path = self.config["intel_file"] if intel_file is None else intel_file
        except KeyError as ex:
            raise KeyError("Missing config.json parameter") from ex

        s3_resource = boto3.resource("s3")

        try:
            if isinstance(data, (list, dict)):
                encoded_data = json.dumps(data).encode()
            elif isinstance(data, str):
                encoded_data = data.encode()
            else:
                encoded_data = data
            res = s3_resource.Object(bucket_name, file_path)
            res = res.put(Body=encoded_data)
            self._healthcheck(data, bucket_name + "/" + file_path, "s3")
        except ClientError as ex:
            raise Exception("Unable to write to S3 ETL Bucket") from ex
        except Exception as ex:
            raise Exception(
                "An Exception occurred while attempting to write to S3"
            ) from ex
            
    def get_s3(
        self,
        key: str,
        bucket: str,
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
        self.logger.debug("Accessing %s in s3 bucket %s", key, bucket)
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
            self.logger.debug(response)
        except ClientError as ex:
            self.logger.error("Could not get from S3: %s", ex)
            raise ex
        else:
            return response["Body"].read()

    def write_humio(
        self,
        data: List,
        metadata: Optional[dict] = None,
        path: Optional[str] = None,
        endpoint: Optional[str] = None,
        token: Optional[str] = None,
        chunk_size: Optional[int] = 200,
        healthcheck: bool = False,
    ) -> None:
        """
        Write intel data to given Humio. Events must be pre-processed (e.g. @timestamp must
        already be formatted)

        :param data: List of data to write to Humio
        :param path: Path to add under _path key per event
        :param endpoint: Select Humio endpoint to write data
        :param token: Humio-generated ingest token
        :param metadata: Optional list of dictionaries for any other information that may be valuable/necessary
        :param healthcheck: indicates the data is a healthcheck

        :raises Exception: general exception for raised exceptions from humio and healthcheck functions
        """

        def _load_batch(
            data: List,
            path: Optional[str] = None,
            metadata: Optional[dict] = None,
            split_by: int = 200,
        ) -> List:
            """
            Formats data as expected by the unstructured Humio Ingest API
            If incoming data is a list greater than split_by (# of records), we will try slicing it

            :param data: data contained in a list
            :param path: path writes to _path for humio parsing
            :param metadata: optional key value pairs to include on every event
            :param split_by: number of records to include in each chunk

            :returns: A list of chunks of events

            :raises TypeError: if data is not of type list
            """
            if not isinstance(data, list):
                raise TypeError(
                    f"Data to write to Humio must be of type 'List' not '{type(data)}'"
                )

            # Assign path to event
            for obj in data:
                obj["_path"] = path
                if metadata is not None:
                    obj.update(metadata)

            # Break items into chunks divided by split_by
            chunks = []
            for i in range(0, len(data), split_by):
                chunk = []
                for event in data[i : i + split_by]:  # noqa:
                    chunk.append(json.dumps(event))
                chunks.append([{"messages": chunk}])
            return chunks

        chunks = _load_batch(data, path, metadata, chunk_size)
        self._send_humio(chunks, endpoint, token, healthcheck)
        if not healthcheck:
            self._healthcheck(data, path, "humio")

    def get_value(
        self,
        path: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "us-west-2",
    ) -> str:
        """
        Gets a single str value from the Parameter Store

        :param path: Name of the path to the SSM parameter
        :param access_key: AWS Access Key (Override in case default credentials don't have permissions to resource)
        :param secret_key: AWS Secret Key (Override in case default credentials don't have permissions to resource)
        :param region: Default region in which to instantiate client

        :returns: The specified str
        """

        def _get(ssm: boto3.client, path: str) -> str:
            """
            Get Value using parameter store path

            :param ssm: instantiation of boto3 client for accessing Parameter store
            :param path: Name of the path to the SSM parameter

            :raises ClientError: When we encounter a problem accessing the specified parameter

            :returns: result of string ssm lookup
            """
            return ssm.get_parameter(Name=path, WithDecryption=True)["Parameter"][
                "Value"
            ]

        if access_key and secret_key:
            # For cases in which SSM parameters are in a different account
            logging.debug("AWS Auth keys present in get_value signature")
            ssm = boto3.client(
                "ssm",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
            )
            return _get(ssm, path)
        else:
            # Otherwise use the default available keys
            logging.debug("Auth keys not preset, using default AWS keys")
            ssm = boto3.client("ssm", region_name=region)
            return _get(ssm, path)

    def get_parameters_of_path(
        self,
        path: str,
        recursive: bool = True,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "us-west-2",
    ) -> Dict:
        """
        Gets dictionary of all the parameters under the given path from the Parameter Store.
        Example of a valid path: /etl-template/endpoints (campus is prepended)

        :param path: Name of the path to the SSM parameter
        :param recursive: Retrieve parameters from any sub paths

        :returns: A dict of all parameters and their value under the path
        """

        def _get(ssm: boto3.client, path: str) -> list:
            """
            Get Value using parameter store path

            :param ssm: instantiation of boto3 client for accessing Parameter store
            :param path: The path to the SSM parameters

            :raises ClientError: When we encounter a problem accessing the specified parameter

            :returns: The list of parameters returned from the ssm lookup
            """
            parameters = []
            next_token = ""
            while True:
                if next_token == "":
                    res = ssm.get_parameters_by_path(
                        Path=path,
                        Recursive=recursive,
                        WithDecryption=True,
                        MaxResults=10,
                    )
                else:
                    res = ssm.get_parameters_by_path(
                        Path=path,
                        Recursive=recursive,
                        WithDecryption=True,
                        MaxResults=10,
                        NextToken=next_token,
                    )

                parameters.extend(res["Parameters"])

                if "NextToken" not in res:
                    return parameters
                next_token = res["NextToken"]

        if access_key and secret_key:
            # For cases in which SSM parameters are in a different account
            logging.debug("AWS Auth keys present in get_value signature")
            ssm = boto3.client(
                "ssm",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
            )
        else:
            # Otherwise use the default available keys
            logging.debug("Auth keys not preset, using default AWS keys")
            ssm = boto3.client("ssm", region_name=region)
        param_dict: Dict[str, str] = {}
        for parameter in _get(ssm, path):
            param_dict[parameter["Name"]] = parameter["Value"]
        return param_dict

    def put_value(
        self,
        path: str,
        value: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        overwrite: bool = True,
        region: str = "us-west-2",
    ) -> None:
        """
        Puts a single str value to the Parameter Store

        :param path: Parameter Store path (or ssm parameter name)
        :param value: Value to store under the indicated path in the Parameter Store
        :param access_key: Access Key provided in the case that the ETL needs special permissions
        :param secret_key: Secret Key provided in the case that the ETL needs special permissions
        :param overwrite: Overwrite existing parameter values, or if false, ignore call
        :param region: Boto3 credential instantiation requires a default region, defaults to Oregon if not specified

        :raises Exception:
        """

        def _put(ssm: boto3.client, path: str, value: str) -> None:
            """
            Puts a single string value to Parameter store

            :param ssm: Instantiated SSM client
            :param path: Path where value will be stored
            :param value: String value that will be put to Parameter

            :raises TypeError: if value is not of type str
            :raises ClientError: if a problem occurs while writing the data
            """
            if not isinstance(value, str):
                raise TypeError(
                    f"Data to write to Parameter Store must be of type 'str' not '{type(value)}'"
                )
            try:
                ssm.put_parameter(
                    Name=path, Value=value, Type="String", Overwrite=overwrite
                )
            except ClientError as ex:
                if ex.response["Error"]["Code"] == "ParameterAlreadyExists":
                    logging.warning(
                        "put_value item not written. Remove overwrite=False to overwrite existing value"
                    )
                else:
                    raise ex

        if access_key and secret_key:
            # For cases in which SSM parameters are in a different account
            logging.debug("AWS Auth keys present in put_value signature")
            ssm = boto3.client(
                "ssm",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
            )
            _put(ssm, path, value)
        else:
            # Otherwise use the default available keys
            logging.debug("Auth keys not preset, using default AWS keys")
            ssm = boto3.client("ssm", region_name=region)
            _put(ssm, path, value)

    @abstractmethod
    def etl(self):
        """Called to run the ETL functionality"""