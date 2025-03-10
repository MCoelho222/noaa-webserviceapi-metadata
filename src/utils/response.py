import numpy as np
from loguru import logger


def process_response(response: dict[str, str], target: str) -> np.ndarray | dict[str, str] | list[str]:
    """Process the response fetched from the NOAA API."

    Args:
        response (dict): The response fetched from the NOAA API.
        target (str): The target to retrieve from the response. 
            Options: 'metadata', 'results', 'ids', 'names', 'ids_names_dict', 'names_ids_dict'.
    """
    try:
        if target == "metadata":
            return response["metadata"]
        elif target == 'results':
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
            logger.error("Failed to process response, Invalid target")
            return response
    except KeyError:
        logger.exception("Failed to process response, KeyError")