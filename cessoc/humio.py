"""
This module is used to send data to Humio.
"""
# TODO add timeout for humio send # pylint: disable=fixme

import os
import json
from typing import List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from botocore.exceptions import ClientError
from cessoc.logging import cessoc_logging


def _send_humio(
    chunked_data: List,
    endpoint: str = None,
    token: str = None,
    session: Optional[requests.sessions.Session] = None
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
    :param session: Session variable to pass in to use to connection pooling

    :raises KeyError: if CAMPUS variable is not set
    :raises Exception: if the configuration is missing for humio connections
    :raises ValueError: if the endpoint or token is 'None' when healthcheck is False
    :raises HTTPError: if any POST request to Humio errors
    """
    logger = cessoc_logging.getLogger("cessoc")
    try:
        if endpoint is None:
            try:
                endpoint = os.environ["ingest_api"]
            except KeyError as ex:
                raise KeyError("endpoint environment variable is undefined") from ex

        # Create a HTTP session
        if session is None:
            session = create_session()

        # Make the ingest POSTs in chunks
        for data in chunked_data:
            resp = session.post(
                endpoint,
                json=data,
                headers={"Authorization": "Bearer " + f"{token}"},
                timeout=120
            )
            resp.raise_for_status()

            logger.info(
                "Event batch of size %s has been sent to Humio", str(len(data[0]['messages']))
            )
    except ClientError as ex:
        raise Exception("Unable to get humio endpoint/ingest_token") from ex  # pylint: disable=raise-missing-from


def write(
    data: List,
    token: str,
    metadata: Optional[dict] = None,
    path: Optional[str] = None,
    endpoint: Optional[str] = None,
    chunk_size: Optional[int] = 200,
    session: Optional[requests.sessions.Session] = None
) -> None:
    """
    Write intel data to given Humio. Events must be pre-processed (e.g. @timestamp must
    already be formatted)

    :param data: List of data to write to Humio
    :param path: Path to add under _path key per event
    :param endpoint: Select Humio endpoint to write data
    :param token: Humio-generated ingest token
    :param metadata: Optional list of dictionaries for any other information that may be valuable/necessary
    :param chunk_size: Number of events to send per POST request to Humio
    :param session: Session variable to pass in to use to connection pooling
    :raises Exception: general exception for raised exceptions from humio functions
    """
    if not isinstance(data, list):
        raise TypeError(
            f"Data to write to Humio must be of type 'List' not '{type(data)}'"
        )

    # Assign path to event
    for obj in data:
        if path is not None:
            obj["_path"] = path
        if metadata is not None:
            obj.update(metadata)

    # Break items into chunks divided by split_by
    chunks = []
    for i in range(0, len(data), chunk_size):
        chunk = []
        for event in data[i: i + chunk_size]:  # noqa:
            chunk.append(json.dumps(event))
        chunks.append([{"messages": chunk}])
    _send_humio(chunks, endpoint, token, session)


def create_session() -> requests.sessions.Session:
    """
    Creates a session for requests to use.
    """
    retry_strategy = Retry(
        total=5,
        backoff_factor=5,
        # backoff_max=30,
        # These status codes indicate something temporarily wrong, fixable by re-request
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    return session
