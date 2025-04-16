from typing import Optional
from loguru import logger

from request import Request
from utils.log import format_log_content
from utils.data import list_of_tuples_from_dict


class NOAALocations(Request):
    """Class to fetch available locations from the NOAA API."""
    def __init__(self) -> None:
        super().__init__("locations")
        self.data = None

    async def fetch_locations(
        self,
        datasetid: Optional[str]=None,
        locationcategoryid: Optional[str]=None,
        datacategoryid: Optional[str]=None,
        startdate: Optional[str]=None,
        enddate: Optional[str]=None,
        sortfield: Optional[str]=None,
        sortorder: Optional[str]=None,
        limit: int=1000,
        offsets: Optional[list[int]]=None) -> dict[str, str]:
        """Fetch the locations from the NOAA API.

        Returns:
            dict: The locations fetched from the NOAA API.
        """
        params_dict = {
            "datasetid": datasetid,
            "locationcategoryid": locationcategoryid,
            "datacategoryid": datacategoryid,
            "startdate": startdate,
            "enddate": enddate,
            "sortfield": sortfield,
            "sortorder": sortorder,
            "limit": limit,
        }
        params_list = list_of_tuples_from_dict(params_dict, exclude_none=True)
        logger.info(format_log_content(context="Fetching locations...", param_tuples=params_list, only_values=True))

        calculated_offsets = offsets
        if offsets is None:
            calculated_offsets = await self.fetch_one_and_calculate_offsets(params_dict)

        data = await self.get_with_offsets(params_dict, calculated_offsets)
        return data


if __name__ == "__main__":
    import asyncio

    async def main():
        noaa_locations = NOAALocations()
        data = await noaa_locations.fetch_locations(datasetid='GSOM', locationcategoryid='CITY')
        if data:
            print(data["metadata"])
            print(len(data["results"]))
    
    asyncio.run(main())