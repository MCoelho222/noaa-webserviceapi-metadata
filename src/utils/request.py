import numpy as np
from typing import Optional

def build_query_string(params: dict[str, str]) -> str:
    """Build a query string from a dictionary of non-none parameters."""
    return "&".join([f"{key}={value}" for key, value in params.items() if value])


def get_params_from_url(url: str, target_params: Optional[list[str]] = None) -> dict[str, str]:
    """Extract specified query parameters from a URL.

    Args:
        target_params (list[str] | None): The list with parameters' names (e.g., ['stationid', 'locationid'])
        url (str): The URL with query parameters

    Returns:
        dict[str, str]: A dictionary with the query parameter as key:value (e.g., {'stationid': 'ABC123', 'locationid': 'FIPS:BR'}).
            If target_params is None, returns a dictionary with all the query parameters.
    """
    # Exract params from URL
    q_params = url.split('?')[1].split('&')


    parsed_params = {}  # Initialize the dictionary of parameters

    # Iterate through the list of URL parameters
    for param in q_params:
        key_value = param.split('=')

        # If target_params is not None and the URL param is in the targets list
        if target_params and key_value[0] in target_params:
            # Include in the dictionary
            parsed_params[key_value[0]] = key_value[1]
        elif not target_params:  # If target_params is None, include all
            parsed_params[key_value[0]] = key_value[1]

    return parsed_params


def calculate_offsets(num: int) -> list[int]:
    """Calculates a list of offset values based on the given number.

    The function divides the input number by 1000 and generates a sequence 
    of offsets from 0 to the next multiple of 1000, incremented by 1000. 
    The first offset is always set to 0.

    Args:
        num (int): The input number to determine offsets.

    Returns:
        list[int]: A list of calculated offsets. If `num` is less than 1000, 
        returns [0].
    """
    n = num // 1000
    if n > 0:
        end = n * 1000 + 1000
        offsets = np.arange(0, end, 1000) + 1
        offsets[0] = 0
        return offsets
    return [0]

if __name__ == "__main__":
    # Test build_query_strings
    base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2/"
    params = {
        "datasetid": "GSOM",
        "stationid": "ABC123",
        "startdate": "2020-01-01",
        "enddate": "2024-12-31"
    }

    url = base_url + "?" + build_query_string(params)

    # Test get_params_from_url
    print(get_params_from_url(url, ['stationid']))
    print(get_params_from_url(url=url))
    
    # Test calculate offsets
    print(build_query_string({'stationid': 'ABC123', 'locationid': 'FIPS:BR'}))
    print((calculate_offsets(18329)))