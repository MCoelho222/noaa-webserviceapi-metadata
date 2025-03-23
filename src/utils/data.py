import re

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


def parse_size_to_human_read(size_bytes) -> str:
    """Return size in appropriate units."""
    # Convert size to appropriate units
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024  # Move to the next unit

    return f"{size_bytes:.2f} TB"  # In case it's huge


def parse_size(size_str: str) -> int:
    """Convert a size string (e.g., '10 MB', '120 B') to bytes (int)."""
    size_units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}

    match = re.fullmatch(r"(?i)\s*(\d+(?:\.\d+)?)\s*([BKMGTP]?B)\s*", size_str.strip())
    if not match:
        raise ValueError(f"Invalid size format: {size_str}")

    size_value, unit = match.groups()
    size_value = float(size_value)  # Convert to float for decimal values
    unit = unit.upper()  # Normalize unit to uppercase (e.g., "kb" -> "KB")

    if unit not in size_units:
        raise ValueError(f"Unknown unit: {unit}")

    return int(size_value * size_units[unit])  # Convert to bytes and return as integer
