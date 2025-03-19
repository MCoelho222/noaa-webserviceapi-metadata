import aiohttp
import asyncio
import os
import numpy as np
from typing import Any, Optional
from loguru import logger

from utils.log import format_log_content
from utils.data import list_of_tuples_from_dict
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
    def __init__(self, endpoint: str, whitelist_path: Optional[str]=None, whitelist_key: Optional[str]=None, whitelist_value: Optional[str]=None):
        super().__init__(whitelist_path, whitelist_key, whitelist_value)
        self.endpoint = endpoint
        self.params_list = None
        self.params_dict = None
        self.has_data = False


    async def get(self, q_string: Optional[str]=None, max_retries: Optional[int]=5) -> Optional[dict]:
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
        token = os.getenv("NOAA_GMAIL_TOKEN")
        if not token:
            logger.error("API token is missing. Set the NOAA_API_TOKEN environment variable.")
            return None

        self.params_dict = self.dict_from_url_params(q_string)
        self.params_list = list_of_tuples_from_dict(self.params_dict)

        # Base URL for the NOAA Web Service API
        baseurl = os.getenv("NOAA_API_URL")

        # Complete URL with endpoint and query parameters, if query parameters are passed
        url = f"{baseurl}{self.endpoint}?{q_string}" if q_string else f"{baseurl}{self.endpoint}"

        semaphore = asyncio.Semaphore(5)  # Max 5 concurrent requests
        async with semaphore:
            await asyncio.sleep(0.2)  # Ensures ~5 requests per second
            async with aiohttp.ClientSession() as session:
                for attempt in range(max_retries):  # Maximum of 5 retries
                    self.has_data = False
                    try:
                        async with session.get(url, headers={"token": token}) as res:
                            if res.status == 503:
                                wait_time = 2 ** attempt  # Exponential backoff
                                logger.debug(f"503 Service Unavailable. Retrying {attempt + 1}/{max_retries} in {wait_time} seconds...")
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
                                    self.has_data = True
                                    results = len(data["results"]) if "results" in data else 0
                                    available = data["metadata"]["resultset"]["count"] if "metadata" in data else 0
                                    logger.success(format_log_content(params=[("Status", 200), ("Items", f"{results}/{available}")]))

                                    if self.endpoint == "data" and not self.is_whitelist_complete:
                                        self._include_in_whitelist()

                                return data

                            except aiohttp.ContentTypeError:
                                logger.error("Failed to parse JSON response")
                                return None

                    except aiohttp.ClientError:
                        logger.exception("Request failed")
                        return None


    async def get_with_offsets(self, params: dict[str, str], offsets: list[int]):
        q_string = self.build_query_string_from_dict(params)

        if len(offsets) == 1:
            data = await self.get(q_string)
        else:
            data = {
                "metadata": {},
                "results": []
            }
            count = 1
            total_count = len(offsets)
            for offset in offsets:
                logger.info(format_log_content(context=f"Fetching offset {count}/{total_count}...", params=[("Endpoint", self.endpoint)]))
                q_string_offset = q_string + f"&offset={offset}"
                res_data = await self.get(self.endpoint, q_string_offset)

                if "metadata" in data and "metadata" not in res_data.keys():
                    data["metadata"] = res_data["metadata"]
                if data and "results" in res_data.keys():
                    data["results"].extend(res_data["results"])

                count += 1
        return data

    
    async def fetch_for_offsets(self, params_dict: dict[str, Any]) -> list[int]:
        logger.info("Fetching for offsets...")
        params_dict["limit"] = 1
        q_string = self.build_query_string_from_dict(params_dict)
        result = await self.get(q_string)

        if result and "metadata" in result.keys():
            count = result["metadata"]["resultset"]["count"]

            return self.calculate_offsets(int(count))
        
        logger.debug("Empty data or 'metadata' not in response")


    def process_response_json(self, res_json: dict[str, str], option: str) -> np.ndarray | dict[str, str] | list[str] | None:
        """Process the response fetched from the NOAA API."

        Args:
            response (dict): The response fetched from the NOAA API.
            option (str): The option to retrieve from the response. 
                Options: 'metadata', 'results', 'ids', 'names', 'ids_names_dict', 'names_ids_dict'.
        """
        if self.has_data:
            try:
                if option == "metadata":
                    return res_json["metadata"]
                elif option == 'results':
                    return res_json["results"]
                elif option == 'ids':
                    # Return ordered list of unique location IDs
                    return np.unique([item["id"] for item in res_json["results"]])
                elif option == 'names':
                    # Return ordered list of unique item names
                    return np.unique([item["name"] for item in res_json["results"]])
                elif option == "ids_names_dict":
                    # Return dictionary with item IDs as keys and item names as values
                    return {item["id"]: item["name"] for item in res_json["results"]}
                elif option == "names_ids_dict":
                    # Return dictionary with item names as keys and item IDs as values
                    return {item["name"]: item["id"] for item in res_json["results"]}
                else:
                    logger.error("Failed to process response, Invalid option")
                    return res_json
            except KeyError:
                logger.exception("Failed to process response, KeyError")
        
        else:
            logger.debug("No response to process")
            return None


    def _include_in_whitelist(self,) -> None:
        # If the response JSON is non-empty, include the whitelist_value in the whitelist_key's list
        if self.whitelist and self.is_whitelist_ready(self.params_list):

            self.add_to_whitelist(
                key=self.params_dict[self.whitelist_key],
                value=self.params_dict[self.whitelist_value],
            )


    @staticmethod
    def build_query_string_from_dict(params_dict: Optional[dict[str, str]]) -> str:
        return "&".join([f"{key}={value}" for key, value in params_dict.items() if value])
    

    @staticmethod
    def dict_from_url_params(url: str, params: Optional[list[str]] = None) -> dict[str, str] | list[tuple[str, str]]:
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

            if len(offsets) > 3:
                log_content = f"Using offsets: [{offsets[0]}, {offsets[1]}, {offsets[2]}, ...,{offsets[-1]}]"
            if len(offsets) == 3:
                log_content = f"Using offsets: [{offsets[0]}, {offsets[1]}, ...,{offsets[-1]}]"
            if len(offsets) == 2:
                log_content = f"Using offsets: [{offsets[0]}, ...,{offsets[-1]}]"
            
            logger.info(log_content)
            return offsets

        logger.info("No offsets needed")
        return [0]
