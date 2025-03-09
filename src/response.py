import numpy as np

from custom_log import custom_logger, build_log_info
from src.logs import LogLevel

def process_response(response: dict[str, str], target: str) -> np.ndarray | dict[str, str] | list[str]:
    """Process the response fetched from the NOAA API."

    Args:
        response (dict): The response fetched from the NOAA API.
        target (str): The target to retrieve from the response. 
            Options: 'metadata', 'results', 'ids', 'names', 'ids_names_dict', 'names_ids_dict'.
    """
    try:
        if target == "metadata":
            # Return the metadata
            return response["metadata"]
        elif target == 'results':
            # Return the list of response
            return response["results"]
        elif target == 'ids':
            # Return ordered list of unique location IDs
            return np.unique([location["id"] for location in response["results"]])
        elif target == 'names':
            # Return ordered list of unique location names
            return np.unique([location["name"] for location in response["results"]])
        elif target == "ids_names_dict":
            # Return dictionary with location IDs as keys and location names as values
            return {location["id"]: location["name"] for location in response["results"]}
        elif target == "names_ids_dict":
            # Return dictionary with location names as keys and location IDs as values
            return {location["name"]: location["id"] for location in response["results"]}
        else:
            log_data = build_log_info(context="Failed to process response", msg="Invalid target")
            custom_logger(log_data, LogLevel.DEBUG)
            return response
    except KeyError:
        log_data = build_log_info(context="Failed to process response", msg="KeyError")
        custom_logger(log_data, LogLevel.EXCEPTION)