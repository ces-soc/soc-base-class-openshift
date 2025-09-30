"""Configures and loads microservice related parameters"""
import sys
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import json
from typing import List, Tuple, Dict

from botocore.exceptions import ClientError
import boto3
from pythonjsonlogger import jsonlogger

from cessoc.configmanager.colors import Colors


class ConfigManager:
    """
    ConfigManager class takes care of credential and configuration parameter gathering for an ephemeral service, and log formatting.

    This manager is meant to ease the setup requirements for writing microservices.
    """

    def __init__(self, force_json_logging=False, file_log=False):
        """Initial configuration"""
        self._file_log = file_log
        self._force_json_logging = force_json_logging

        self.config = {}

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        root_logger = logging.getLogger()
        # default to info until setup occurs
        root_logger.setLevel(logging.INFO)

        self.handler = self.get_logging_handler()

        # this service should be the only thing configuring the root logger
        # services like AWS lambda automatically configure the root logger and that should be overridden by this setup
        if len(root_logger.handlers) != 0:
            root_logger.handlers = []
        root_logger.addHandler(self.handler)

        # set log format
        if isatty() and self._force_json_logging is False:
            # if terminal, set color output
            self.handler.setFormatter(
                CustomColorFormatter(
                    "[%(asctime)s] [%(levelname)-19s] [%(lineno)04d] [%(name)-25s] [%(funcName)-15s] %(message)s"
                )
            )
        else:
            # if non-terminal, set json output
            self.handler.setFormatter(
                jsonlogger.JsonFormatter(
                    "%(asctime)s %(levelname)s %(lineno)d %(name)s %(funcName)s %(message)s"
                )
            )

    def get_logging_handler(self):
        """Gets the corresponding logging handler based on the type configured for the class. Can be a stdout or file logger"""
        if self._file_log:
            # if logging to a file, do so in json
            self._force_json_logging = True
            # Create the rotating file handler. Limit the size to 1000000Bytes ~ 1MB .
            return RotatingFileHandler("log.json", maxBytes=1000000, backupCount=2)
        else:
            # set logging to stdout
            return logging.StreamHandler(sys.stdout)

    def _set_loggers(self, logger_levels: List[Tuple[str, int]]):
        available_loggers = logging.root.manager.loggerDict.keys()
        self.logger.debug("Available loggers: %s", ",".join(available_loggers))

        for name, level in logger_levels:
            if name in available_loggers:
                self.logger.debug("Setting %s logging to %s", name, level)
                logging.getLogger(name).setLevel(level)
            else:
                raise ValueError(f"Logger {name} does not exist")

    def load(
        self, config_path: str, load_campus: bool = True, logger_levels: Dict = None
    ) -> None:
        """
        Loads the configs and retrieves their values
        This section will setup the root logger with proper logging configuration.
        The only configurable setting is the logging severity which will can be adjusted using `LOGGING_SEVERITY` in the config.json.
        If logging_severity is not set in the config, it defaults to 'info' for easier testing

        :param config_path: Path to the config location
        :param load_campus: Whether to try and load the campus environment variable
        :param logger_levels: Logger level configuration. Default: {"botocore": logging.ERROR, "urllib3": logging.ERROR}
        :raises KeyError: If the campus environment variable doesn't exist
        """
        levels: Dict = {"botocore": logging.ERROR, "urllib3": logging.ERROR}
        if logger_levels:
            for name, level in logger_levels.items():
                levels[name] = level

        self.config = self._load(config_path)
        self.config = self._resolve_variables(self.config)

        # map str to logging level
        logging_severity_dict = {
            "info": logging.INFO,
            "debug": logging.DEBUG,
            "warning": logging.WARNING,
        }

        # default to info
        logging_level = "info"
        if "logging_severity" in self.config:
            logging_level = self.config["logging_severity"].lower()

        # set root logging level
        logging.getLogger().setLevel(logging_severity_dict[logging_level])
        self.logger.setLevel(logging_severity_dict[logging_level])
        self.logger.info("Setting logging level to %s", logging_level)

        set_levels: List[Tuple[str, int]] = []
        for name, level in levels.items():
            set_levels.append((name, level))
        self._set_loggers(set_levels)

        # set campus name
        if load_campus:
            try:
                self.config["campus_name"] = os.environ["CAMPUS"].lower()

            except KeyError as e:
                raise KeyError(
                    "The OS environment variable 'CAMPUS' must be available"
                ) from e

        if "parameters" not in self.config:
            self.config["parameters"] = {}

        if "scope" in self.config:
            self.config["parameters"].update(
                self._resolve_aws_parameters_scope(self.config["scope"])
            )

        if "extra_parameters" in self.config:
            self.config["parameters"].update(
                {
                    **self._resolve_aws_parameters(self.config["extra_parameters"]),
                    **self.config["parameters"],
                }
            )

        json_parameters = {}
        for name, value in self.config["parameters"].items():
            try:
                json_parameters[name] = json.loads(value)
            except json.JSONDecodeError:
                self.logger.debug("Could not decode %s as json", name)
                json_parameters[name] = value
        self.config["parameters"] = json_parameters

    def _load(self, config_path: str) -> Dict:
        """Load the configuration file as a dict"""
        try:
            with open(config_path, "r") as config_file:
                self.logger.debug("Loading %s", config_path)
                return json.loads(config_file.read())
        except json.JSONDecodeError as e:
            raise ValueError("Could not decode '{}' as json".format(config_path)) from e

    def _resolve_variables(self, config: Dict) -> Dict:
        """Casts the config obj as a string and resolves any variables ${var} using the environment variables"""
        raw = json.dumps(config)
        hits = []

        # Search for ${ and } and capture anything between
        for match in re.finditer(r"\$\{([A-z]*?)\}", raw):
            hits.append((match.group(0), match.group(1)))

        # Remove duplicates
        hits = set(hits)

        # Find and replace any occurrence of hits
        try:
            for hit in hits:
                self.logger.debug("Resolving config variable %s", hit)
                raw = raw.replace(hit[0], os.environ[hit[1]])
        except KeyError as e:
            # environment variables must be available
            raise ValueError(
                "Env variable {} is empty or unavailable".format(hit[1])
            ) from e

        self.logger.debug("Config object after parsing: %s", raw)

        # reload config with resolved variables
        return json.loads(raw)

    def _resolve_aws_parameters_scope(self, scopes: list) -> Dict:
        """
        Resolves AWS SSM parameters by scope
        :param scopes: The scopes to query by
        :raises AttributeError: If an invalid parameter was provided
        :raises ValueError: If the parameter cannot be resolved
        :returns: The resolved parameters
        """
        resolved_parameters = {}
        try:
            # get parameter store connection
            for scope in scopes:
                ssm = boto3.client("ssm", region_name="us-west-2")
                next_token = ""
                while True:
                    if next_token == "":
                        res = ssm.get_parameters_by_path(
                            Path=scope,
                            Recursive=True,
                            WithDecryption=True,
                            MaxResults=10,
                        )
                    else:
                        res = ssm.get_parameters_by_path(
                            Path=scope,
                            Recursive=True,
                            WithDecryption=True,
                            MaxResults=10,
                            NextToken=next_token,
                        )

                    # add parameters to the config
                    for parameter in res["Parameters"]:
                        self.logger.debug(
                            "Resolving parameter: '%s'", parameter["Name"]
                        )
                        resolved_parameters[
                            remove_ssm_prefix(parameter["Name"])
                        ] = parameter["Value"]

                    if "NextToken" not in res:
                        break
                    next_token = res["NextToken"]
        except ClientError as ex:
            # must be able to resolve parameters
            raise ValueError(
                f"Unable to resolve parameters. Are your AWS credentials valid? Exception: {ex}"
            ) from ex

        # set resolved parameters in the configuration
        return resolved_parameters

    def _resolve_aws_parameters(self, parameters: Dict) -> Dict:
        """
        Retrieves values from the AWS Parameter Store
        :param parameters: The parameters to load from AWS
        :raises AttributeError: If an invalid parameter was provided
        :raises ValueError: If the parameter cannot be resolved
        :returns: The resolved parameters
        """
        # check if any parameters are listed to resolve
        if len(parameters) == 0:
            self.logger.debug("No parameters provided to resolve")
            return {}

        resolved_parameters = {}
        try:
            # get parameter store connection
            ssm = boto3.client("ssm", region_name="us-west-2")

            # query for parameters in chunks
            chunk_size = 9
            for i in range(0, len(parameters), chunk_size):
                # loop through array slices
                for chunk in [parameters[i : i + chunk_size]]:
                    res = ssm.get_parameters(Names=chunk, WithDecryption=True)

                    # Exception is not raised for invalid parameters, so raise one here
                    if res["InvalidParameters"]:
                        raise AttributeError(
                            "Invalid Parameters: {}".format(
                                json.dumps(res["InvalidParameters"])
                            )
                        )

                    # add parameters to the config
                    for parameter in res["Parameters"]:
                        self.logger.debug(
                            "Resolving parameter: '%s'", parameter["Name"]
                        )
                        resolved_parameters[
                            remove_ssm_prefix(parameter["Name"])
                        ] = parameter["Value"]
        except ClientError as ex:
            # must be able to resolve parameters
            raise ValueError(
                "Unable to resolve parameters. Are your AWS credentials valid?"
            ) from ex

        # set resolved parameters in the configuration
        return resolved_parameters


def remove_ssm_prefix(value: str) -> str:
    """Remove ssm prefix. /ces/service/name -> service/name"""
    return re.sub(r"^/[a-z]+/", "", value)


def isatty() -> bool:
    """
    Check if stdout is going to a terminal
    :returns: True or False if current tty is a terminal
    """
    if (hasattr(sys.stdout, "isatty") and sys.stdout.isatty()) or (
        "TERM" in os.environ and os.environ["TERM"] == "ANSI"
    ):
        return True
    return False


class CustomColorFormatter(logging.Formatter):
    """Colored Logging"""

    def __init__(self, *args, **kwargs):
        """Creates colored logging"""
        super().__init__(*args, **kwargs)
        self.colors = Colors()
        self.color_list = [
            Colors.BLUE,
            Colors.BROWN,
            Colors.PURPLE,
            Colors.CYAN,
            Colors.LIGHT_BLUE,
            Colors.LIGHT_PURPLE,
            Colors.LIGHT_CYAN,
        ]
        self.modules = {}

    def format(self, record):
        """Override for logging formatter"""
        if record.levelno == logging.DEBUG:
            record.levelname = self.colors.color(record.levelname, Colors.DARK_GRAY)
            record.msg = self.colors.color(record.msg, Colors.DARK_GRAY)
        elif record.levelno == logging.INFO:
            record.levelname = self.colors.color(record.levelname, Colors.GREEN)
            record.msg = self.colors.color(record.msg, Colors.GREEN)
        elif record.levelno == logging.WARNING:
            record.levelname = self.colors.color(record.levelname, Colors.YELLOW)
            record.msg = self.colors.color(record.msg, Colors.YELLOW)
        elif record.levelno == logging.ERROR:
            record.levelname = self.colors.color(record.levelname, Colors.RED)
            record.msg = self.colors.color(record.msg, Colors.RED)
        elif record.levelno == logging.CRITICAL:
            record.levelname = self.colors.color(record.levelname, Colors.LIGHT_RED)
            record.msg = self.colors.color(record.msg, Colors.LIGHT_RED)

        if record.name in self.modules:
            record.name = self.colors.color(record.name, self.modules[record.name])
        else:
            self.modules[record.name] = self.color_list[
                len(self.modules) % len(self.color_list)
            ]
            record.name = self.colors.color(record.name, self.modules[record.name])

        return super().format(record)


if __name__ == "__main__":
    # configmanager generally shouldn't be run standalone
    # this is to help with adhoc testing
    c = ConfigManager()
    # this assumes there a config.json
    c.load("config.json")