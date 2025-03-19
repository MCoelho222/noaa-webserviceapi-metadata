from typing import Any


def dict_from_list_of_tuples(params: list[tuple[str, str]]) -> dict[str, str]:
    """Build a dictionary from a list of tuples."""
    return {key: value for key, value in params}


def list_of_tuples_from_dict(obj: dict[str, Any], exclude_none: bool=False) -> list[tuple[str, str]]:
    """Convert a dictionary to a list of tuples.

    Args:
        obj (dict[str, Any]): The dictionary to be converted to a list of tuples.

    Returns:
        list[tuple[str, str]]: A list of tuples with key-value pairs.
    """
    obj_list = []
    for key, value in obj.items():
        if exclude_none:
            if value:
                obj_list.append((key, value))
        else:
            obj_list.append((key, value))


    return obj_list