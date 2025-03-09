import numpy as np
from typing import Optional

from src.utils.request import build_query_string, make_http_request
from src.custom_log import custom_logger, LogLevel, build_log_info


async def fetch_locations(
    datasetid: Optional[str]=None,
    locationcategoryid: Optional[str]=None,
    datacategoryid: Optional[str]=None,
    startdate: Optional[str]=None,
    enddate: Optional[str]=None,
    sortfield: Optional[str]=None,
    sortorder: Optional[str]=None,
    limit: Optional[str]=1000,
    offset: Optional[str]=0,
    verbose:Optional[bool]=0) -> dict[str, str]:
    """Fetch the locations from the NOAA API.

    Args:
        dataset_id (str): The dataset ID.
        locationcategoryid (str): The location category ID.
        datacategoryid (str): The data category ID.
        startdate (str): The start date for the query.
        enddate (str): The end date for the query.
        sortfield (str): The field to sort the results.
        sortorder (str): The order to sort the results.
        limit (int): The limit parameter as specified in the NOAA Web Service API documentation.
        offset (int):The offset parameter as specified in the NOAA Web Service API documentation.
    
    Returns:
        dict: The locations fetched from the NOAA API.
    """
    q_params = {
        "datasetid": datasetid,
        "locationcategoryid": locationcategoryid,
        "datacategoryid": datacategoryid,
        "startdate": startdate,
        "enddate": enddate,
        "sortfield": sortfield,
        "sortorder": sortorder,
        "limit": limit,
        "offset": offset
    }

    # Build the query string with the non-none parameters
    q_string = build_query_string(q_params)

    # Get all the available locations
    data = await make_http_request("locations", q_string)

    if verbose and data and "metadata" in data.keys():
        metadata = data["metadata"]
        log_data = build_log_info(
            context="Fetch locations",
            params=[("Items", f"{len(data['results'])}/{metadata['count']}"),]
        )
        custom_logger(log_data, LogLevel.INFO)

    return data


def process_locations(locations: dict[str, str], target: str) -> np.ndarray | dict[str, str] | list[str]:
    """Process the locations fetched from the NOAA API."

    Args:
        locations (dict): The locations fetched from the NOAA API.
        target (str): The target to retrieve from the locations. 
            Options: 'metadata', 'results', 'ids', 'names', 'ids_names_dict', 'names_ids_dict'.
    """
    try:
        if target == "metadata":
            return locations["metadata"]
        elif target == 'results':
            return locations["results"]
        elif target == 'ids':
            # Return ordered list of unique location IDs
            return np.unique([location["id"] for location in locations["results"]])
        elif target == 'names':
            # Return ordered list of unique location names
            return np.unique([location["name"] for location in locations["results"]])
        elif target == "ids_names_dict":
            # Return dictionary with location IDs as keys and location names as values
            return {location["id"]: location["name"] for location in locations["results"]}
        elif target == "names_ids_dict":
            # Return dictionary with location names as keys and location IDs as values
            return {location["name"]: location["id"] for location in locations["results"]}
        else:
            log_data = build_log_info(context="Failed to process locations", msg="Invalid target")
            custom_logger(log_data, LogLevel.DEBUG)
            return locations
    except KeyError:
        log_data = build_log_info(context="Failed to process locations", msg="KeyError")
        custom_logger(log_data, LogLevel.EXCEPTION)