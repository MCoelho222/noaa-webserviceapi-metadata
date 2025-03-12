import numpy as np
from typing import Any


def list_to_dict(params: list[tuple[str, str]]) -> dict[str, str]:
    """Build a dictionary from a list of tuples."""
    return {key: value for key, value in params}


def dict_to_list(obj: dict[str, Any]) -> list[tuple[str, str]]:
    """Convert a dictionary to a list of tuples.

    Args:
        obj (dict[str, Any]): The dictionary to be converted to a list of tuples.

    Returns:
        list[tuple[str, str]]: A list of tuples with key-value pairs.
    """
    obj_list = []
    for key, value in obj.items():
        obj_list.append((key, value))

    return obj_list