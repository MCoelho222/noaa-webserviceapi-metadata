from typing import Optional

from request import send_get_request
from utils.params import build_query_string


async def fetch_locations(
    datasetid: Optional[str]=None,
    locationcategoryid: Optional[str]=None,
    datacategoryid: Optional[str]=None,
    startdate: Optional[str]=None,
    enddate: Optional[str]=None,
    sortfield: Optional[str]=None,
    sortorder: Optional[str]=None,
    limit: Optional[str]=1000,
    offset: Optional[str]=0) -> dict[str, str]:
    """Fetch the locations from the NOAA API.

    Args:
        dataset_id (str): The dataset ID.
        locationcategoryid (str): The location category ID.
        datacategoryid (str): The data category ID.
        startdate (str): The start date for the query.
        enddate (str): The end date for the query.
        sortfield (str): The field to sort the results.
        sortorder (str): The order to sort the results.
        limit (int): The limit parameter as specified in the NOAA Web Service API documentation.
        offset (int):The offset parameter as specified in the NOAA Web Service API documentation.
    
    Returns:
        dict: The locations fetched from the NOAA API.
    """
    q_params = {
        "datasetid": datasetid,
        "locationcategoryid": locationcategoryid,
        "datacategoryid": datacategoryid,
        "startdate": startdate,
        "enddate": enddate,
        "sortfield": sortfield,
        "sortorder": sortorder,
        "limit": limit,
        "offset": offset
    }

    # Build the query string with the non-none parameters
    q_string = build_query_string(q_params)

    # Get all the available locations
    data = await send_get_request("locations", q_string)

    return data


if __name__ == "__main__":
    import asyncio

    async def main():
        locations = await fetch_locations(datasetid='GSOM', locationcategoryid='CITY')
        if locations:
            print(locations["metadata"])
            print(locations["results"][:5])
    
    asyncio.run(main())