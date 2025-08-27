"""
This module contains miscellaneous utility functions for the cessoc package.
"""
import base64


def bytes_to_str(value: bytes) -> str:
    """
    :param value: A bytes object
    :return: The string representation of the bytes
    """
    byte = base64.b64encode(value)
    byte_str = byte.decode("utf-8")
    return byte_str


def str_to_bytes(value: str) -> bytes:
    """
    :param value: A string object (containing bytes)
    :return: The bytes representation of the string
    """
    byte = base64.b64decode(value)
    return byte
