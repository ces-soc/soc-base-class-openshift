"""
This module is used for logging.
"""
import sys
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import List, Tuple
from pythonjsonlogger import jsonlogger


class cessoc_logging:
    """This module is for logging"""
    logging_setup = False
    logger = None

    def _set_root_logging(force_json_logging=False, file_log=False):  # pylint: disable=E0213
        """
        Sets up the root logger so it will properly output data.
        """
        root_logger = logging.getLogger()
        logging_severity_dict = {
            "info": logging.INFO,
            "debug": logging.DEBUG,
            "warning": logging.WARNING,
        }
        if "LOGGING_SEVERITY" in os.environ and os.environ["LOGGING_SEVERITY"] in logging_severity_dict:  # pylint: disable=E0213
            root_logger.setLevel(logging_severity_dict[os.environ["LOGGING_SEVERITY"]])
        else:
            root_logger.setLevel(logging.INFO)
        handler = cessoc_logging._get_logging_handler(file_log)
        if len(root_logger.handlers) != 0:
            root_logger.handlers = []
        root_logger.addHandler(handler)
        # set log format
        if "LOGGING_FORMAT" in os.environ and os.environ["LOGGING_FORMAT"].lower() == "ansi" and force_json_logging is False:  # pylint: disable=E0213
            # if terminal, set single line output
            handler.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] [%(levelname)-19s] [%(lineno)04d] [%(name)-25s] [%(funcName)-15s] %(message)s"
                )
            )
        else:
            # if non-terminal, set json output
            handler.setFormatter(
                jsonlogger.JsonFormatter(
                    "%(asctime)s %(levelname)s %(lineno)d %(name)s %(funcName)s %(message)s"
                )
            )
        cessoc_logging.logging_setup = True

    def _get_logging_handler(file_log):  # pylint: disable=E0213
        """Gets the corresponding logging handler based on the type configured for the class. Can be a stdout or file logger"""
        if file_log:
            # Create the rotating file handler. Limit the size to 1000000Bytes ~ 1MB .
            return RotatingFileHandler("log.json", maxBytes=1000000, backupCount=2)
        # set logging to stdout
        return logging.StreamHandler(sys.stdout)

    def getLogger(name, force_json_logging=False, file_log=False):  # pylint: disable=E0213
        """
        This gets a logger with the name specified. If the root logger has not been configured, it will do so.
        This only configures the root logger the first time it is configured.
        """
        if not cessoc_logging.logging_setup:
            cessoc_logging._set_root_logging(force_json_logging=force_json_logging, file_log=file_log)
            cessoc_logging.logger = logging.getLogger("cessoc")
        if name == "cessoc":
            return cessoc_logging.logger
        return logging.getLogger(name)

    def setLogger(logger_levels: List[Tuple[str, int]]):  # pylint: disable=E0213
        """Allows setting the log level of other loggers. This can be useful if an SDK is noisy."""
        available_loggers = logging.root.manager.loggerDict.keys()

        for name, level in logger_levels:
            if name in available_loggers:
                logging.getLogger(name).setLevel(level)
            else:
                raise ValueError(f"Logger {name} does not exist")
