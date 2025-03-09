import aiohttp
import asyncio
import os
from dotenv import load_dotenv
from loguru import logger
from typing import Optional

from src.utils.request import build_query_string, get_params_from_url
from whitelist import add_to_whitelist
from src.logs import LogLevel
from custom_log import custom_logger, build_log_info

load_dotenv()


async def make_http_request(
    endpoint: str,
    q_params: Optional[str] = None,
    whitelist_path: Optional[str] = None,
    loc_complete: bool=False) -> Optional[dict]:
    """Asynchronous function for making HTTP requests to the NOAA Web Services API.

    It ensures a maximum of 5 concurrent requests (NOAA API's limit).

    Args:
        endpoint (str): One of ['datasets', 'datacategories', 'datatypes', 'locationcategories', 'locations', 'stations', 'data'].
        q_params (str, optional): Query parameters for the request URL (e.g., 'datasetid=GSOM&locationid=FIPS:BR').
        whitelist_path (str, optional): The path to the whitelist JSON file.
        loc_complete (bool, optional): If True, the location is considered complete.

    Returns:
        dict or None: The parsed content of the response object, or None if the request fails.
    """
    CONTEXT = "Fetch"
    # Retrieve token from .env
    token = os.getenv("NOAA_TOKEN")
    if not token:
        logger.error("API token is missing. Set the NOAA_API_TOKEN environment variable.")
        return None

    # Base URL for the NOAA Web Service API
    baseurl = os.getenv("NOAA_API_URL")

    # Complete URL with endpoint and query parameters, if query parameters are passed
    url = f"{baseurl}{endpoint}?{q_params}" if q_params else f"{baseurl}{endpoint}"

    semaphore = asyncio.Semaphore(5)  # Max 5 concurrent requests
    async with semaphore:
        await asyncio.sleep(0.2)  # Ensures ~5 requests per second
        async with aiohttp.ClientSession() as session:
            for attempt in range(5):  # Maximum of 5 retries
                try:
                    async with session.get(url, headers={"token": token}) as res:
                        # Extract 'stationid' and 'locationid' from the URL
                        params_dict = get_params_from_url(url, ["stationid", "locationid"])
                        station_id = params_dict.get("stationid")
                        location_id = params_dict.get("locationid")

                        if res.status == 503:
                            wait_time = 2 ** attempt  # Exponential backoff

                            # Log retry according to available query parameters
                            if location_id and station_id:
                                log_data = build_log_info(
                                    location=location_id.split(':')[-1],
                                    station=station_id,
                                    context=CONTEXT,
                                    msg=f"503 Service Unavailable. Retrying in {wait_time} seconds...")

                            elif location_id and not station_id:
                                log_data = build_log_info(
                                    location=location_id.split(':')[-1],
                                    context=CONTEXT,
                                    msg=f"503 Service Unavailable. Retrying in {wait_time} seconds...")
                            else:
                                log_data = build_log_info(context=CONTEXT, msg=f"503 Service Unavailable. Retrying in {wait_time} seconds...")

                            custom_logger(log_data, LogLevel.WARNING)

                            await asyncio.sleep(wait_time)
                            continue  # Retry the request

                        # Just log if status is not 200
                        if res.status != 200:
                            log_data = build_log_info(context=CONTEXT, msg=f"Status {res.status}")
                            custom_logger(log_data, LogLevel.ERROR)

                            if q_params:
                                log_data = build_log_info(context=CONTEXT, extra_params=[("Query params", {', '.join(q_params.split('&'))})])
                                custom_logger(log_data, LogLevel.ERROR)

                            return None

                        # If status code is 200
                        try:
                            data = await res.json()

                            # If the response JSON is non-empty, include the station in the whitelist
                            if data and whitelist_path:
                                params_dict = get_params_from_url(url, ["stationid", "locationid"])

                                if "stationid" in params_dict.keys() and "locationid" in params_dict.keys():
                                    add_to_whitelist(
                                        whitelist_path=whitelist_path,
                                        loc=location_id.split(":")[-1],
                                        station_id=station_id,
                                        loc_complete=loc_complete
                                        )
                                else:
                                    log_data = build_log_info(context=CONTEXT, msg="Cannot add to whitelist. No station ID in the URL")
                                    custom_logger(log_data, LogLevel.DEBUG)

                            # or if is empty but the stations screening is complete
                            if not data and whitelist_path and loc_complete:
                                add_to_whitelist(
                                        whitelist_path=whitelist_path,
                                        loc=location_id.split(":")[-1],
                                        station_id=None,
                                        loc_complete=loc_complete
                                        )

                            # If response JSON is empty, just log
                            if not data:
                                params_dict = get_params_from_url(url, ["stationid"])
                                if "stationid" in params_dict.keys():
                                    station_id = params_dict["stationid"]
                                    log_data = build_log_info(context=CONTEXT, station=station_id, msg="Empty station")
                                    custom_logger(log_data, LogLevel.DEBUG)
                                else:
                                    log_data = build_log_info(context=CONTEXT, msg="Empty data")
                                    custom_logger(log_data, LogLevel.DEBUG)

                            # Return the data, whether it's empty or not
                            return data

                        except aiohttp.ContentTypeError:
                            log_data=build_log_info(context=CONTEXT, msg="Failed to parse JSON response")
                            custom_logger(log_data, LogLevel.ERROR)
                            return None

                except aiohttp.ClientError as e:
                    log_data = build_log_info(context=CONTEXT, msg=f"Request failed: {e}")
                    custom_logger(log_data, LogLevel.EXCEPTION)
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
        WHITELIST_TEST_PATH = "whitelist_test.json"
        result = await make_http_request('data', q_params, WHITELIST_TEST_PATH)
        return result
    
    asyncio.run(main())