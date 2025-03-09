from datetime import datetime
from typing import Any, Optional

from src.logs import LogLevel
from utils.request import build_query_string, make_http_request
from custom_log import custom_logger, build_log_info
from whitelist import retrieve_whitelist

async def fecth_data(
    datasetid: str,
    startdate: str,
    enddate: str,
    stationid: Optional[str]=None,
    datatypeid: Optional[str]=None,
    locationid: Optional[str]=None,
    units: Optional[str]=None,
    sortfield: Optional[str]=None,
    sortorder: Optional[str]=None,
    limit: Optional[str]=1000,
    offset: Optional[int]=0,
    includemetadata: Optional[bool]=True,
    verbose:Optional[bool]=0) -> dict[str, str] | None:
    """Fetch the data from the NOAA API.

    Args:
        datasetid (str): The dataset ID.
        startdate (str): The start date for the query.
        enddate (str): The end date for the query.
        stationid (str, optional): The station ID.
        datatypeid (str, optional): The datatype ID.
        locationid (str, optional): The location ID.
        units (str, optional): The units parameter as specified in the NOAA Web Service API documentation.
        sortfield (str, optional): The field to sort the results.
        sortorder (str, optional): The order to sort the results.
        limit (int, optional): The limit parameter as specified in the NOAA Web Service API documentation.
        offset (int, optional): The offset parameter as specified in the NOAA Web Service API documentation.
        includemetadata (bool, optional): The metadata flag.
        verbose (bool, optional): The verbosity flag.
    
    Returns:
        Optional[dict[str, str]]: A dictionary with 'metadata' and 'results' keys or None.
    """
    q_params = {
        "datasetid": datasetid,
        "startdate": startdate,
        "enddate": enddate,
        "stationid": stationid,
        "datatypeid": datatypeid,
        "locationid": locationid,
        "units": units,
        "sortfield": sortfield,
        "sortorder": sortorder,
        "limit": limit,
        "offset": offset,
        "includemetadata": includemetadata,
    }

    # Build the query string with the non-none parameters
    q_string = build_query_string(q_params)

    # Get all the available locations in the location category within the specified time range
    data = await make_http_request("data", q_string)

    if verbose and data and "metadata" in data.keys():
        metadata = data["metadata"]
        log_data = build_log_info(
            context="Fetch data",
            params=[("Items", f"{len(data['results'])}/{metadata['count']}"),]
        )
        custom_logger(log_data, LogLevel.INFO)

    return data


async def fetch_data_from_stations_by_location(
    datasetid: str,
    startdate: str,
    enddate: str,
    locationid: str,
    stationids: list[str],
    datatypeid: Optional[str | list[str]]=None,
    units: Optional[str]=None,
    sortfield: Optional[str]=None,
    sortorder: Optional[str]=None,
    limit: Optional[str]=1000,
    offset: Optional[int]=0,
    includemetadata: Optional[bool]=True,
    whitelist_path: Optional[str]=None,
    verbose: Optional[bool]=0) -> list[dict[str, Any]]:
    """Fetches station data for a specific country within a date range.

    This function retrieves station data from an external source for a given dataset,
    country location, a list of station IDs, and time range. It checks if the location is
    already in the whitelist, and if all its stations were already screened. If yes,
    it replaces the stations list provided by the one in the whitelist. Otherwise, it proceeds
    with the fecthing and whitelist updating operations. Additionally, if 'step_months' is
    provided, the time range is split into intervals, to avoid reaching the items max-limit per request.

    Args:
        datasetid (str): The ID of the dataset to query.
        startdate (str): The start date for data retrieval (format: "YYYY-MM-DD").
        enddate (str): The end date for data retrieval (format: "YYYY-MM-DD").
        locationid (str): The location identifier for the country.
        limit (Optional[int], default=1000): The maximum number of records per request.
        stations_ids (list[str]): A list of station IDs to fetch data for.
        whitelist_path (Optional[str]): The path to the whitelist file (if available).
        whitelist_key (Optional[str]): The key to use in the whitelist file.
        verbose (Optional[bool], default=False): A flag to enable verbose logging.

    Returns:
        list[dict[str, Any]]: A list of dictionaries containing station data.

    Raises:
        ValueError: If `startdate` is after `enddate`.
        FileNotFoundError: If the whitelist file is not found.
    """
    # Validate dates
    start = datetime.strptime(startdate, "%Y-%m-%d")
    end = datetime.strptime(enddate, "%Y-%m-%d")
    if start > end:
        raise ValueError("Start date must be before end date")
    
    q_params = {
        "datasetid": datasetid,
        "startdate": startdate,
        "enddate": enddate,
        "datatypeid": datatypeid,
        "locationid": locationid,
        "units": units,
        "sortfield": sortfield,
        "sortorder": sortorder,
        "limit": limit,
        "offset": offset,
        "includemetadata": includemetadata,
    }

    # Build the query string with the non-none parameters
    q_string = build_query_string(q_params)

    if verbose:
        # Log the starting of fetch operations
        log_data = build_log_info(context="Fetch data", extra_params=[("Dataset", datasetid), ("Location", locationid)])
        custom_logger(log_data, LogLevel.INFO)

    # Try to retrieve whitelist for the given location (e.g., 'BR')
    try:
        whitelist = retrieve_whitelist(whitelist_path, locationid)
    except FileNotFoundError:
        whitelist = None

    # If the location's whitelist is complete,
    if whitelist:
        is_complete = whitelist["metadata"] == "C"
        if is_complete:
            # redefine 'stationids' to include only the ones in the whitelist
            stationids = [station_id for station_id in stationids if station_id in whitelist[locationid]]

    complete_dataset = []  # Store all the data

    # Stations counter to track when all station have been sreened
    stations_count = 0
    is_complete = False
    for station_id in stationids:
        try:
            # Add station id to query string
            q_string = q_string + f"&stationid={station_id}"

            if stations_count == len(stationids) - 1:
                is_complete = True

            # Fetch and process results
            result = await make_http_request('data', q_string, whitelist_path, is_complete)
            data = None
            if result:
                data = result['results']
                complete_dataset.extend(data)

            if verbose:
                items = len(data) if data else 0
                total_items = len(complete_dataset)
                log_data = build_log_info(
                    location=locationid,
                    station=station_id,
                    msg="Data fetched" if data else "Empty data",
                    context="Fetch data",
                    extra_params=[("Items", items), ("Total items", total_items)])
                level = LogLevel.SUCCESS if data else LogLevel.DEBUG
                custom_logger(log_data, level)

            stations_count += 1  # Increase stations counter
        except Exception:
            log_data = build_log_info(
                location=locationid,
                station=station_id,
                context="Fetch data",
                extra_params=[("Period", f"{startdate} - {enddate}")])
            custom_logger(log_data, LogLevel.EXCEPTION)

    return complete_dataset

if __name__ == "__main__":
    import asyncio

    
