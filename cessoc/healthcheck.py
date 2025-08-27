"""
The healthcheck package provides standard healthcheck functionality for cessoc services.
"""
import os
import atexit
import sys
import time
from datetime import datetime, timedelta
from typing import Optional, Union
import tzlocal
from cessoc import humio
import requests

from cessoc.logging import cessoc_logging


class OpenshiftHealthCheck:
    """
    Sends information to humio with pre-defined fields in addition to a custom field.
    """
    def __init__(self, service_name: str, endpoint: Optional[str] = None, token: Optional[str] = None):
        """
        Initializes the healthcheck object.

        :param service_name: The name of the service sending the healthcheck
        """

        self.start_time = time.time()
        self.service_name = service_name
        self.campus = os.environ["CAMPUS"].lower()
        self.timezone = tzlocal.get_localzone()
        self.endpoint = endpoint
        self.token = token
        atexit.register(self._end)

        self._logger = cessoc_logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def _end(self):
        """
        Sends healthcheckt data to Humio after the service has ended. Or crashed.
        """

        try:  # gets the last uncaught exception see https://docs.python.org/3/library/sys.html#sys.exc_info
            error = str(sys.last_type) + str(sys.last_value)
        except AttributeError:
            error = None
        if error is None:
            self.send(status="completed", endpoint=self.endpoint, token=self.token)
        else:
            self.send(status="errored", custom_data={"error": error}, endpoint=self.endpoint, token=self.token)

    def send(self, custom_data: Union[str, dict] = "None", endpoint: Optional[str] = None, token: Optional[str] = None, status="running", session: Optional[requests.sessions.Session] = None):
        """
        Sends the healthcheck data to humio. This will run automatically when the program exits. This can be called on a long running service to send periodic healthcheck data.
        :param custom_data: The custom data to be sent to humio. Must be a json object
        :param token: The humio ingest token
        :param endpoint: The humio ingest endpoint
        :param status: The status of the service. Defaults to "running". Can be "running", "errored", or "completed"
        """
        if token is None:
            token = os.environ["healthcheck_ingest_token"]
        if status not in ["running", "errored", "completed"]:  # check if status is valid
            raise ValueError("status must be 'running', 'errored', or 'completed'")
        # set end time
        self.end_time = time.time()
        # Make the runtime easily readable
        readable_runtime = timedelta(seconds=round(self.end_time - self.start_time)).__str__().split(":")
        readable_runtime = f"{readable_runtime[0]}h {readable_runtime[1]}m {readable_runtime[2]}s"

        # data to send to humio healthcheck
        healthdata = [{
            "start_time": f"{datetime.fromtimestamp(self.start_time, tz=self.timezone)}",
            "end_time": f"{datetime.fromtimestamp(self.end_time, tz=self.timezone)}",
            "service_name": f"{self.service_name}",
            "status": f"{status}",
            "data": custom_data,
            "runtime": f"{readable_runtime}",
            "@timestamp": f"{self.end_time}"
        }]
        if os.getenv("CAMPUS") is not None:
            healthdata[0]['campus'] = os.getenv("CAMPUS")
        if os.getenv("STAGE") is not None:
            healthdata[0]['env'] = os.getenv("STAGE")

        self._logger.info("sending healthcheck data to humio")
        humio.write(data=healthdata, endpoint=endpoint, token=token, path="healthcheck", session=session)
