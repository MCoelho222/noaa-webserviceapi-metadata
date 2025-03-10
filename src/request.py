import aiohttp
import asyncio
import os
from dotenv import load_dotenv
from loguru import logger
from typing import Optional

from utils.log import build_log_info
from utils.params import build_query_string, get_params_from_url
from whitelist import add_to_whitelist

load_dotenv()


async def send_get_request(
    endpoint: str,
    q_params: Optional[str] = None,
    whitelist_path: Optional[str] = None,
    whitelist_key: Optional[str] = None,
    whitelist_value: Optional[str] = None,
    whitelist_complete: bool=False) -> Optional[dict]:
    """Asynchronous function for making HTTP requests to the NOAA Web Services API.

    It ensures a maximum of 5 concurrent requests (NOAA API's limit).

    Args:
        endpoint (str): One of ['datasets', 'datacategories', 'datatypes', 'locationcategories', 'locations', 'stations', 'data'].
        q_params (str, optional): Query parameters for the request URL (e.g., 'datasetid=GSOM&locationid=FIPS:BR').
        whitelist_path (str, optional): The path to the whitelist JSON file.
        whitelist_complete (bool, optional): If True, the location is considered complete.

    Returns:
        dict or None: The parsed content of the response object, or None if the request fails.
    """
    # Retrieve token from .env
    token = os.getenv("NOAA_GMAIL_TOKEN")
    if not token:
        logger.error("API token is missing. Set the NOAA_API_TOKEN environment variable.")
        return None

    params_list = get_params_from_url(q_params, mode="list")
    params_list.extend([("whitelist_path", whitelist_path), ("whitelist_key", whitelist_key), ("whitelist_value", whitelist_value)])

    # Ensure whitelist_key and whitelist_value are in the query parameters
    if (whitelist_path and not whitelist_key) or (whitelist_path and not whitelist_value):
        logger.error("Both whitelist_key and whitelist_value must be provided")
        return None
    if (whitelist_key and not whitelist_value) or (whitelist_value and not whitelist_key):
        logger.error("Both whitelist_key and whitelist_value must be provided")
        return None
    if whitelist_key and whitelist_key not in [param[0] for param in params_list]:
        logger.error("Missing whitelist_key in the query parameters")
        return None
    if whitelist_value and whitelist_value not in [param[0] for param in params_list]:
        logger.error("Missing whitelist_value in the query parameters")
        return None
    
    # Base URL for the NOAA Web Service API
    baseurl = os.getenv("NOAA_API_URL")

    # Complete URL with endpoint and query parameters, if query parameters are passed
    url = f"{baseurl}{endpoint}?{q_params}" if q_params else f"{baseurl}{endpoint}"

    semaphore = asyncio.Semaphore(5)  # Max 5 concurrent requests
    async with semaphore:
        await asyncio.sleep(0.2)  # Ensures ~5 requests per second
        async with aiohttp.ClientSession() as session:
            context = "Started..."
            for attempt in range(5):  # Maximum of 5 retries
                log_content = build_log_info(context=context, params=params_list)
                try:
                    async with session.get(url, headers={"token": token}) as res:
                        logger.info(log_content)

                        if res.status == 503:
                            wait_time = 2 ** attempt  # Exponential backoff
                            logger.debug(f"503 Service Unavailable. Retrying in {wait_time} seconds...")
                            context = "Retrying..."
                            await asyncio.sleep(wait_time)
                            continue  # Retry the request

                        # Just log if status is not 200
                        if res.status != 200:
                            logger.error(f"Status {res.status}")

                            return None

                        try:  # If status code is 200, try to parse the JSON response
                            data = await res.json()
                            if not data:  # If response JSON is empty, just log
                                logger.debug("Empty data")
                            else:
                                downloaded = len(data["results"]) if "results" in data else 0
                                available = data["metadata"]["resultset"]["count"] if "metadata" in data else 0
                                log_content = build_log_info(context="Downloaded", params=[("Status", res.status), ("Count", f"{downloaded}/{available}"),])
                                logger.success(log_content)

                            # If the response JSON is non-empty, include the whitelist_value in the whitelist_key's list
                            if whitelist_path:
                                # Extract the key and value from the query parameters
                                params_dict = get_params_from_url(url, [whitelist_key, whitelist_value], mode="dict")
                                key = params_dict[whitelist_key]

                                # or if is empty but the screening is complete,
                                # we have to call the function with value=None to update the whitelist's metadata
                                value = None if not data and whitelist_complete else params_dict[whitelist_value]

                                if key and value:
                                    add_to_whitelist(
                                        whitelist_path=whitelist_path,
                                        key=key,
                                        value=value,
                                        is_whitelist_complete=whitelist_complete
                                        )

                            return data

                        except aiohttp.ContentTypeError:
                            logger.error("Failed to parse JSON response")
                            return None

                except aiohttp.ClientError:
                    logger.exception("Request failed")
                    return None


if __name__ == "__main__":
    import asyncio

    params = {
        "datasetid": "GSOM",
        "locationid": "FIPS:BR",
        "stationid": "GHCND:BR000352000",
        "startdate": "2020-01-01",
        "enddate": "2024-12-31",
        "limit": 1000
    }
    q_params = build_query_string(params)

    async def main():
        result = await send_get_request('data', q_params, "whitelist_test.json", "locationid", "stationid", False)
        return result
    
    asyncio.run(main())