import aiohttp
import asyncio
import os
import numpy as np
from typing import Optional
from loguru import logger

from utils.log import formatted_log_content
from src.utils.data import dict_to_list
from whitelist import Whitelist


class Request(Whitelist):
    """
    Class for making HTTP GET requests to the NOAA Web Services API.

    Attributes:
        whitelist_path (str, optional): The path to the whitelist JSON file.
        whitelist_key (str, optional): The key to be used in the whitelist.
        whitelist_value (str, optional): The value to be used in the whitelist.
        is_whitelist_complete (bool): If True, the location is considered complete.
    """
    def __init__(self, whitelist_path: Optional[str]=None, whitelist_key: Optional[str]=None, whitelist_value: Optional[str]=None):
        super().__init__(whitelist_path, whitelist_key, whitelist_value)
        self.params = {}
        self.response = None

    async def get(self, endpoint: str, q_string: Optional[str]=None, max_retries: Optional[int]=5) -> Optional[dict]:
        """Asynchronous function for making HTTP GET requests to the NOAA Web Services API.

        It ensures a maximum of 'max_retries' concurrent requests (NOAA API's limit).

        Args:
            endpoint (str): One of ['datasets', 'datacategories', 'datatypes', 'locationcategories', 'locations', 'stations', 'data'].
            q_string (str, optional): Query parameters for the request URL (e.g., 'datasetid=GSOM&locationid=FIPS:BR').
            whitelist_key (str, optional): The key to be used in the whitelist.
            whitelist_value (str, optional): The value to be used in the whitelist.
            is_whitelist_complete (bool, optional): If True, the location is considered complete.
            max_retries (int, optional): The maximum number of retries if the request
                fails with a 503 status code.

        Returns:
            dict or None: The parsed content of the response object, or None if the request fails.
        """
        # Retrieve token from .env
        token = os.getenv("NOAA_GMAIL_TOKEN")
        if not token:
            logger.error("API token is missing. Set the NOAA_API_TOKEN environment variable.")
            return None

        self.params = self.url_params_to_dict(q_string)

        params_list = dict_to_list(self.params)
        params_list.extend([("whitelist_key", self.whitelist_key), ("whitelist_value", self.whitelist_value)])

        # Base URL for the NOAA Web Service API
        baseurl = os.getenv("NOAA_API_URL")

        # Complete URL with endpoint and query parameters, if query parameters are passed
        url = f"{baseurl}{endpoint}?{q_string}" if q_string else f"{baseurl}{endpoint}"

        semaphore = asyncio.Semaphore(5)  # Max 5 concurrent requests
        async with semaphore:
            await asyncio.sleep(0.2)  # Ensures ~5 requests per second
            async with aiohttp.ClientSession() as session:
                context = "Started..."
                for attempt in range(max_retries):  # Maximum of 5 retries
                    log_content = formatted_log_content(context=context, params=params_list)
                    try:
                        async with session.get(url, headers={"token": token}) as res:
                            logger.info(log_content)

                            if res.status == 503:
                                wait_time = 2 ** attempt  # Exponential backoff
                                logger.debug(f"503 Service Unavailable. Retrying in {wait_time} seconds...")
                                context = f"Retrying... {attempt + 1}/{max_retries}"
                                await asyncio.sleep(wait_time)
                                continue  # Retry the request

                            if res.status != 200:
                                logger.error(f"Status {res.status}")
                                return None

                            try:  # If status code is 200, try to parse the JSON response
                                data = await res.json()

                                if not data:
                                    logger.debug("Empty data")
                                else:
                                    results = len(data["results"]) if "results" in data else 0
                                    available = data["metadata"]["resultset"]["count"] if "metadata" in data else 0
                                    log_content = formatted_log_content(context="200", params=[("Items", f"{results}/{available}"),])
                                    logger.success(log_content)

                                # If the response JSON is non-empty, include the whitelist_value in the whitelist_key's list
                                if self.is_whitelist_ready(params_list):
                                    # Extract the key and value from the query parameters
                                    params_dict = self.url_params_to_dict(url, [self.whitelist_key, self.whitelist_value])
                                    key = params_dict[self.whitelist_key]

                                    # Or if is empty but the screening is complete,
                                    # we have to call the function with value=None to update the whitelist's metadata
                                    value = None if not data and self.is_whitelist_complete else params_dict[self.whitelist_value]

                                    if key and value:
                                        self.add_to_whitelist(
                                            key=key,
                                            value=value,
                                            is_whitelist_complete=self.is_whitelist_complete
                                            )

                                self.response = data
                                return data

                            except aiohttp.ContentTypeError:
                                logger.error("Failed to parse JSON response")
                                return None

                    except aiohttp.ClientError:
                        logger.exception("Request failed")
                        return None
    

    def process_response(self, option: str) -> np.ndarray | dict[str, str] | list[str]:
        """Process the response fetched from the NOAA API."

        Args:
            response (dict): The response fetched from the NOAA API.
            option (str): The option to retrieve from the response. 
                Options: 'metadata', 'results', 'ids', 'names', 'ids_names_dict', 'names_ids_dict'.
        """
        if self.response:
            try:
                if option == "metadata":
                    return self.response["metadata"]
                elif option == 'results':
                    return self.response["results"]
                elif option == 'ids':
                    # Return ordered list of unique location IDs
                    return np.unique([location["id"] for location in self.response["results"]])
                elif option == 'names':
                    # Return ordered list of unique location names
                    return np.unique([location["name"] for location in self.response["results"]])
                elif option == "ids_names_dict":
                    # Return dictionary with location IDs as keys and location names as values
                    return {location["id"]: location["name"] for location in self.response["results"]}
                elif option == "names_ids_dict":
                    # Return dictionary with location names as keys and location IDs as values
                    return {location["name"]: location["id"] for location in self.response["results"]}
                else:
                    logger.error("Failed to process response, Invalid option")
                    return self.response
            except KeyError:
                logger.exception("Failed to process response, KeyError")
        
        else:
            logger.debug("No response to process")
            return None


    def build_query_string_from_dict(self, params_dict: Optional[dict[str, str]]) -> str:
        params_dict = params_dict if params_dict else self.params

        return "&".join([f"{key}={value}" for key, value in params_dict.items() if value])
    

    @staticmethod
    def url_params_to_dict(url: str, params: Optional[list[str]] = None) -> dict[str, str] | list[tuple[str, str]]:
        """Extract specified query parameters from a URL.

        Args:
            params (list[str] | None): A list with parameters' names to be included (e.g., ['stationid', 'itemid']).
            url (str): The URL with query parameters
            mode (str): The return mode. If
                'dict', returns a dictionary with the query parameter as key:value. Default is 'dict'.
                'list', returns a list of tuples. Default is 'dict'.

        Returns:
            dict[str, str] | list[tuple[str, str]]: A dictionary with the query parameter as key:value.
                If params is None, returns a dictionary with all the query parameters.
                If mode is 'list', returns a list of tuples.
        """
        # Exract params from URL
        url_split_len = len(url.split('?'))
        q_params = url.split('?')[1].split('&') if url_split_len > 1 else url.split('?')[0].split('&')
        parsed_params = {}  # Initialize the dictionary of parameters
        # Iterate through the list of URL parameters
        for param in q_params:
            key_value = param.split('=')

            # If params is not None and the URL param is in the targets list
            if params and key_value[0] in params:
                # Include in the dictionary
                parsed_params[key_value[0]] = key_value[1]
            elif not params:  # If params is None, include all
                parsed_params[key_value[0]] = key_value[1]

        return parsed_params


    @staticmethod
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
