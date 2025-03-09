from typing import Optional

from logs import LogLevel
from utils.request import build_query_string
from request import make_http_request
from custom_log import custom_logger, build_log_info


async def fetch_stations(
    datasetid: Optional[str]=None,
    locationid: Optional[str]=None,
    datacategoryid: Optional[str]=None,
    datatypeid: Optional[str]=None,
    extent: Optional[str]=None,
    startdate: Optional[str]=None,
    enddate: Optional[str]=None,
    sortfield: Optional[str]=None,
    sortorder: Optional[str]=None,
    limit: Optional[str]=1000,
    offset: Optional[int]=0,
    verbose:Optional[bool]=0) -> dict[str, str] | None:
    """Download the available stations according to the specified parameters.

    Args:
        datasetid (str): The dataset ID.
        locationid (str, optional): The location ID.
        datacategoryid (str, optional): The datacategory ID.
        datatypeid (str, optional): The datatype ID.
        extent (str, optional): The extent parameter as specified in the NOAA Web Service API documentation.
        startdate (str, optional): The start date for the query.
        enddate (str, optional): The end date for the query.
        sortfield (str, optional): The field to sort the results.
        sortorder (str, optional): The order to sort the results.
        limit (int, optional): The limit parameter as specified in the NOAA Web Service API documentation.
        offset (int, optional): The offset parameter as specified in the NOAA Web Service API documentation.
        verbose (bool, optional): The verbosity flag.

    Returns:
        Optional[dict[str, str]]: A dictionary with 'metadata' and 'results' keys or None.
    """
    all_params = {
        "datasetid": datasetid,
        "locationid": locationid,
        "datacategoryid": datacategoryid,
        "datatypeid": datatypeid,
        "extent": extent,
        "startdate": startdate,
        "enddate": enddate,
        "sortfield": sortfield,
        "sortorder": sortorder,
        "limit": limit,
        "offset": offset
    }
    q_string = build_query_string(all_params)

    # Fetch station's endpoint
    data = await make_http_request("stations", q_string)

    if verbose and data and "metadata" in data.keys():
        metadata = data["metadata"]
        log_data = build_log_info(
            context="Fetch stations",
            params=[("Items", f"{len(data['results'])}/{metadata["count"]}"),]
        )
        custom_logger(log_data, LogLevel.INFO)

    return data 

if __name__ == "__main__":
    import asyncio

    async def main():
        # Test
        stations = await fetch_stations(dataset_id='GSOM', location_id='FIPS:BR')
        if stations:
            print(stations["metadata"])
            print(len(stations["results"]))
            stations = await fetch_stations(dataset_id='GSOM', location_id='FIPS:BR')
            print(type(stations))
    
    asyncio.run(main())