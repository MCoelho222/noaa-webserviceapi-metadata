from loguru import logger
from typing import Optional

def dict_from_url_params(url: str, target_params: Optional[list[str]]=None) -> dict[str, str | int]:
    """Extract all or specified query parameters from the URL.

    Args:
        params (list[str] | None): A list with parameters' names to be retrieved from the URL (e.g., ['stationid', 'itemid']).
        url (str): The URL with query parameters

    Returns:
        dict[str, str | int]: A dictionary with the query parameter as key: value pairs.
            If params is None, returns a dictionary with all the query parameters.
    """
    if len(url.split('?')) != 2:
        raise ValueError(f"Malformed URL: {url}")
    elif not url.split('?')[1]:
        raise ValueError(f"No query parameters in the URL: {url}")

    q_params = url.split('?')[1].split('&')
    if target_params is not None:
        q_params = [q_param for q_param in q_params if q_param.split('=')[0] in target_params]
        
    parsed_params = {}
    for param in q_params:
        key_value = param.split('=')
        if len(key_value) != 2:
            logger.debug(f"Malformed query param {param}, URL: {url}")
            continue

        parsed_params[key_value[0]] = key_value[1]

    return parsed_params
