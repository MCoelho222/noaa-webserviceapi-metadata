import json
import os
from typing import Optional

from custom_log import custom_logger, build_log_info
from src.logs import LogLevel


def add_to_whitelist(whitelist_path: str, key: str, value: Optional[str]=None, loc_complete: bool=False) -> None:
    """Includes a station ID in a JSON file (the whitelist).

    The whitelist contains a JSON with the following structure: 

    {
        "metadata": {
            "BR": "C",
            "US": "I"
        },
        "BR": [abs123, abc456, ...],
        "US": [abs123, abc456, ...]
    }

    "C" stands for "complete" and "I" for "incomplete". A location is complete when all
    the available stations have been fetched and the ones with actual data are included in the whitelist.

    Args:
        whitelist_path (str): The path to the whitelist JSON file.
        loc (str): The location associated with the station (e.g., 'BR')
        station_id (str, optional): The station ID. If None, the location is considered complete.
        loc_complete (bool, optional): If True, the location is considered complete.
    """
    CONTEXT = "Whitelist"
    
    try:
        # Create the whitelist if it doesn't exist
        if not os.path.exists(whitelist_path):
            # Initiate the json
            init_json = {
                "metadata": {key: "I"},
                key: [value,]
            }  

            with open(whitelist_path, "w") as f:
                json.dump(init_json, f, indent=4)

            log_data = build_log_info(context=CONTEXT, msg="Created", params=[("Key", key), ("Value", value)])
            custom_logger(log_data, LogLevel.INFO)

        else:  # Append the station ID to the location's list

            # Load the whitelist JSON
            with open(whitelist_path, "r") as f:
                whitelist = json.load(f)

            # Check if the location exists in the whitelist
            if key in whitelist.keys():
                if value is None:  # Location is complete, update metadata
                    whitelist["metadata"][key] = "C"
                    log_data = build_log_info(context=CONTEXT, msg="Whitelist complete", params=[("Key", key), ("Value", value)])
                    custom_logger(log_data, LogLevel.SUCCESS)

                # Check if the station ID is already included in the location's whitelist
                elif value not in whitelist[key]:
                    # Include in the whitelist
                    whitelist[key].append(value)

                    if loc_complete:
                        whitelist["metadata"][key] = "C"
                        log_data = build_log_info(context=CONTEXT, msg="Whitelist complete", params=[("Key", key), ("Value", value)])
                        custom_logger(log_data, LogLevel.SUCCESS)
                    else:
                        log_data = build_log_info(context=CONTEXT, msg="Appended", params=[("Key", key), ("Value", value)])
                        custom_logger(log_data, LogLevel.INFO)

                elif value in whitelist[key]:  # Just log if it's already included
                    if loc_complete:
                        whitelist["metadata"][key] = "C"
                        log_data = build_log_info(context=CONTEXT, msg="Already in whitelist, location complete", params=[("Key", key), ("Value", value)])
                        custom_logger(log_data, LogLevel.WARNING)
                    else:
                        log_data = build_log_info(context=CONTEXT, msg="Already in whitelist", params=[("Key", key), ("Value", value)])
                        custom_logger(log_data, LogLevel.DEBUG)

            else:  # Create location's key if it doesn't exist
                whitelist["metadata"][key] = "I" if not loc_complete else "C"
                whitelist[key] = [value,]

                log_data = build_log_info(context=CONTEXT, msg="Appended", params=[("Key", key), ("Value", value)])
                custom_logger(log_data, LogLevel.INFO)

            # Update the whitelist file with the updated JSON
            with open(whitelist_path, "w") as f:
                json.dump(whitelist, f, indent=4)

    except Exception:
        custom_logger(build_log_info(context=CONTEXT, msg="Failed creating or appending to the whitelist"), LogLevel.EXCEPTION)


def retrieve_whitelist(whitelist_path: str, loc: Optional[str] = None) -> dict[str, str]:
    """Retrive the station IDs from a given location or the complete whitelist JSON.

    Args:
        whitelist_path (str): The path to the whitelist JSON file.
        loc (str): The location associated with the station (e.g., 'BR')

    Returns:
        dict[str, str]: A dictionary with the metadata and stations for the given location,
            or empty if the location doesn't exist. Also, returns the complete whitelist if no location is given.

    Raises:
        FileNotFoundError: If the path doesn't exist.
    """
    # Load the whitelist JSON
    if os.path.exists(whitelist_path):
        with open(whitelist_path, "r") as f:
            whitelist = json.load(f)

        if not loc:  # Return the complete whitelist if no location is given
            return whitelist

        # If loc was given and exists as a whitelist key, return the associated stations' list
        if loc in whitelist.keys():
            stations = [station.replace('\n', '') for station in whitelist[loc]]
            return {
                "metadata": whitelist["metadata"][loc],
                loc: stations
            }

        return {}
    else:
        raise FileNotFoundError(f"The path doesn't exist: {whitelist_path}")


if __name__ == "__main__":
    WHITELIST_TEST_PATH = "whitelist_test.json"

    add_to_whitelist(WHITELIST_TEST_PATH, "AU", "ABC123")
    add_to_whitelist(WHITELIST_TEST_PATH, "BR", "ABC123")
    add_to_whitelist(WHITELIST_TEST_PATH, "BR", "ABC123")
    add_to_whitelist(WHITELIST_TEST_PATH, "US", "ABC123")
    add_to_whitelist(WHITELIST_TEST_PATH, "US", "ABC123", loc_complete=True)
    add_to_whitelist(WHITELIST_TEST_PATH, "US", "DEF456", loc_complete=True)

    with open(WHITELIST_TEST_PATH, "r") as f:
        data = json.load(f)

    print(data)

    try:
        print(retrieve_whitelist(WHITELIST_TEST_PATH, "BR"))
        print(retrieve_whitelist(WHITELIST_TEST_PATH, "AR"))
        print(retrieve_whitelist(WHITELIST_TEST_PATH))
    except FileNotFoundError as e:
        print(e)