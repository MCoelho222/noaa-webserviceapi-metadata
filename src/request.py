import aiohttp
import asyncio
import os
import numpy as np
from typing import Optional
from loguru import logger

from utils.log import build_log_info
from utils.params import extract_query_params
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

        self.response = None

    async def get_request(self, endpoint: str, q_string: Optional[str]=None, max_retries: Optional[int]=5) -> Optional[dict]:
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

        params_list = extract_query_params(q_string, mode="list")
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
                    log_content = build_log_info(context=context, params=params_list)
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
                                    log_content = build_log_info(context="200", params=[("Items", f"{results}/{available}"),])
                                    logger.success(log_content)

                                # If the response JSON is non-empty, include the whitelist_value in the whitelist_key's list
                                if self.is_whitelist_ready(params_list):
                                    # Extract the key and value from the query parameters
                                    params_dict = extract_query_params(url, [self.whitelist_key, self.whitelist_value], mode="dict")
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
