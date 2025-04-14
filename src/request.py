import aiohttp
import asyncio
import os
import json
import numpy as np
import xml.etree.ElementTree as ET
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
    def __init__(
        self,
        endpoint: str,
        whitelist_path: Optional[str]=None,
        whitelist_key: Optional[str]=None,
        whitelist_value: Optional[str]=None,
        whitelist_title: Optional[str]=None,
        whitelist_description: Optional[str]=None) -> None:
        super().__init__(whitelist_path, whitelist_key, whitelist_value, whitelist_title, whitelist_description)
        self.endpoint = endpoint
        self.requests_count = 0  # Counter for the number of requests made
        self.success_count = 0  # Counter for the number of successful requests


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
        token = os.getenv("NOAA_HOTMAIL_TOKEN")
        if not token:
            logger.error("API token is missing. Set the NOAA_API_TOKEN environment variable.")
            return None

        baseurl = os.getenv("NOAA_API_URL")  # Base URL for the NOAA Web Services API
        q_string = self.build_query_string_from_dict(q_params)
        url = f"{baseurl}{self.endpoint}?{q_string}" if q_string else f"{baseurl}{self.endpoint}"

        semaphore = asyncio.Semaphore(5)  # Max 5 concurrent requests
        async with semaphore:
            await asyncio.sleep(0.2)  # Ensures ~5 requests per second
            async with aiohttp.ClientSession() as session:
                for attempt in range(max_retries):  # Maximum of 5 retries
                    try:
                        async with session.get(url, headers={"token": token}) as res:
                            self.requests_count += 1  # Increment the request count
                            if res.status == 503:
                                wait_time = 2 ** attempt  # Exponential backoff
                                logger.debug(f"503 Service Unavailable. Retrying {attempt + 1}/{max_retries} in {wait_time} seconds...")
                                await asyncio.sleep(wait_time)
                                continue  # Retry the request
                            if res.status != 200:
                                try:
                                    error_text = await res.text()

                                    # Parse XML and extract <developerMessage>
                                    try:
                                        root = ET.fromstring(error_text)
                                        dev_msg = root.findtext('developerMessage')
                                        message = dev_msg or "Unknown error"
                                    except ET.ParseError:
                                        message = f"Unparseable XML: {error_text}"

                                except Exception as e:
                                    message = f"Could not read response body: {e}"

                                logger.error(f"Status {res.status}: {message}")
                                return None

                            try:  # If status code is 200, try to parse the JSON response
                                self.success_count += 1  # Increment the success count
                                data = await res.json()

                                if not data:
                                    logger.debug("Empty data")
                                elif "metadata" in data.keys():
                                    size_bytes = len(json.dumps(data["results"]).encode("utf-8"))  # Convert JSON to bytes
                                    available = data["metadata"]["resultset"]["count"]
                                    logger.success(format_log_content(params=[("Status", 200), ("Returned items", f"{len(data["results"])}/{available}")]))

                                    # The whitelist is used for the 'data' endpoint only
                                    if self.endpoint == "data" and self.whitelist and not self.is_sub_whitelist_complete:
                                        self.add_to_whitelist(
                                            key=q_params[self.whitelist_key],
                                            value=q_params[self.whitelist_value],
                                            metadata={
                                                "items": len(data["results"]),
                                                "size": size_bytes
                                            }
                                        )
                                return data
                            except aiohttp.ContentTypeError:
                                logger.error("Failed to parse JSON response")
                                return None
                    except aiohttp.ClientError:
                        logger.exception("Request failed")
                        return None


    async def get_with_offsets(self, q_params: dict[str, str], offsets: list[int]):
        if len(offsets) == 0:
            raise ValueError("'offsets' should not be empty")
        
        offsets_length = len(offsets)
        all_data = {}
        count = 1  # Keep track of offsets
        results = []

        for offset in offsets:
            if offsets_length > 1:
                q_params["offset"] = offset
                logger.info(format_log_content(context=f"Fetching offset {count}/{offsets_length}...", params=[("Endpoint", self.endpoint)]))

            data = await self.get(q_params)

            if data and "metadata" in data.keys():
                if not results:  # Since all responses will contain the same metadata, include only the first one
                    all_data["metadata"] = data["metadata"]
                results.extend(data["results"])

            count += 1

        if results:
            all_data["results"] = results

        return all_data

    
    async def fetch_one_and_calculate_offsets(self, q_params: dict[str, Any]) -> list[int]:
        logger.info("Fetching for offsets...")
        limited_q_params = q_params.copy()
        limited_q_params["limit"] = 1
        result = await self.get(q_params)

        if result:
            if result and "metadata" in result.keys():
                count = result["metadata"]["resultset"]["count"]

                return self.calculate_offsets(int(count))
        
        logger.debug("Empty data or 'metadata' not in response")
        return [0]


    @staticmethod
    def process_response_json(
        res_json: dict[str, dict[str, str | int] | list[dict[str, str]]],
        option: str) -> dict[str, str | int] | list[dict[str, str]] | np.ndarray[str] | list[str]:
        """Process a response fetched from the NOAA API."

        Args:
            res_json (dict[str, dict[str, str | int] | list[dict[str, str]]]): The response fetched from the NOAA API.
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
    def build_query_string_from_dict(params_dict: dict[str, str | int]) -> str:
        if params_dict:
            return "&".join([f"{key}={value}" for key, value in params_dict.items() if value])
        else:
            return ""


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
                content = f"[{offsets[0]}, {offsets[1]}, {offsets[2]}, ..., {offsets[-1]}]"
            if len(offsets) == 3:
                content = f"[{offsets[0]}, {offsets[1]}, ..., {offsets[-1]}]"
            if len(offsets) == 2:
                content = f"[{offsets[0]}, {offsets[-1]}]"
            
            logger.info("Using offsets: " + content)
            return offsets

        logger.info("Offsets not required")
        return [0]

if __name__ == "__main__":
    import time

    async def main():
        endpoint = "data"
        req = Request(endpoint)
        stations = ["GHCND:AE000041196", "GHCND:AEM00041194", "GHCND:AEM00041217", "GHCND:AEM00041218", "GHCND:MUM00041242", "GHCND:MUM00041242"]
        tasks = [req.get(q_params={"datasetid": "GSOM", "startdate": "2024-01-01", "enddate": "2025-01-01", "stationid": station, "locationid": "FIPS:AE"}) for station in stations]
        start_all = time.time()
        results = await asyncio.gather(*tasks)
        print(results)
        end_all = time.time()
        print(f"Time for all requests: {end_all - start_all}")
    
    asyncio.run(main())

