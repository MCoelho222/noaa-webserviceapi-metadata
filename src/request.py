import aiohttp
import asyncio
import os
import numpy as np
from typing import Any, Optional
from loguru import logger

from utils.log import format_log_content
from whitelist import Whitelist


class Request(Whitelist):
    """
    Class for making HTTP GET requests to the NOAA Web Services API.

    It inherits from the Whitelist class. The Request class can optionally use a whitelist to store
    query parameters that return actual non-empty data.

    Attributes:
        endpoint (str): The endpoint to be fetched.
        whitelist_path (str, optional): The path where the whitelist should be saved or loaded from.
            If not provided, whitelist will not be used.
        whitelist_key (str, optional): The query parameter that represents a key in the whitelist.
        whitelist_value (str, optional): The query parameter that represents a value in the whitelist.
    """
    def __init__(self, endpoint: str, whitelist_path: Optional[str]=None, whitelist_key: Optional[str]=None, whitelist_value: Optional[str]=None):
        super().__init__(whitelist_path, whitelist_key, whitelist_value)
        self.endpoint = endpoint
        self.has_data = False


    async def get(self, q_params: Optional[dict[str, str]]=None, max_retries: Optional[int]=5) -> Optional[dict]:
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

        baseurl = os.getenv("NOAA_API_URL")  # Base URL for the NOAA Web Services API
        q_string = self.build_query_string_from_dict(q_params)
        url = f"{baseurl}{self.endpoint}?{q_string}" if q_string else f"{baseurl}{self.endpoint}"

        self.has_data = False

        semaphore = asyncio.Semaphore(5)  # Max 5 concurrent requests
        async with semaphore:
            await asyncio.sleep(0.2)  # Ensures ~5 requests per second
            async with aiohttp.ClientSession() as session:
                for attempt in range(max_retries):  # Maximum of 5 retries
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

                                    # The whitelist is used for the 'data' endpoint only
                                    if self.endpoint == "data":
                                        if self.whitelist and not self.is_whitelist_complete:
                                            if self.is_whitelist_ready(q_params):
                                                self.add_to_whitelist(
                                                    key=q_params[self.whitelist_key],
                                                    value=q_params[self.whitelist_value])
                                return data
                            except aiohttp.ContentTypeError:
                                logger.error("Failed to parse JSON response")
                                return None
                    except aiohttp.ClientError:
                        logger.exception("Request failed")
                        return None


    async def get_with_offsets(self, q_params: dict[str, str], offsets: list[int]):
        if len(offsets) == 1:
            data = await self.get(q_params)
        else:
            data = {"metadata": {}, "results": []}
            count = 1  # Keep track of offsets
            total_count = len(offsets)
            for offset in offsets:
                logger.info(format_log_content(context=f"Fetching offset {count}/{total_count}...", params=[("Endpoint", self.endpoint)]))

                q_params["offset"] = offset
                res_data = await self.get(self.endpoint, q_params)

                if "metadata" in data and "metadata" not in res_data.keys():
                    data["metadata"] = res_data["metadata"]
                if data and "results" in res_data.keys():
                    data["results"].extend(res_data["results"])

                count += 1

        return data

    
    async def check_offsets_need(self, q_params: dict[str, Any]) -> list[int]:
        logger.info("Fetching for offsets...")
        q_params["limit"] = 1
        result = await self.get(q_params)
        if result and "metadata" in result.keys():
            count = result["metadata"]["resultset"]["count"]

            return self.calculate_offsets(int(count))
        
        logger.debug("Empty data or 'metadata' not in response")


    @staticmethod
    def process_response_json(res_json: dict[str, str], option: str) -> np.ndarray | dict[str, str] | list[str] | None:
        """Process a response fetched from the NOAA API."

        Args:
            response (dict): The response fetched from the NOAA API.
            option (str): The option to retrieve from the response. 
                Options: 'metadata', 'results', 'ids', 'names', 'ids_names_dict', 'names_ids_dict'.
        """
        try:
            if option == "metadata":
                return res_json["metadata"]
            elif option == 'results':
                return res_json["results"]
            elif option == 'ids':
                return np.unique([item["id"] for item in res_json["results"]])
            elif option == 'names':
                return np.unique([item["name"] for item in res_json["results"]])
            elif option == "ids_names_dict":
                return {item["id"]: item["name"] for item in res_json["results"]}
            elif option == "names_ids_dict":
                return {item["name"]: item["id"] for item in res_json["results"]}
            else:
                logger.error("Failed to process response, Invalid option")
                return res_json
        except KeyError:
            logger.exception("Failed to process response, KeyError")


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
        url_split_len = len(url.split('?'))
        q_params = url.split('?')[1].split('&') if url_split_len > 1 else url.split('?')[0].split('&')
        parsed_params = {}
        for param in q_params:
            key_value = param.split('=')
            if params and key_value[0] in params:
                parsed_params[key_value[0]] = key_value[1]
            elif not params:
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
