from typing import Optional
from loguru import logger

from request import Request


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
        offset: int=0) -> dict[str, str]:
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
        logger.info("Fetching locations...")

        calculated_offsets = [offset]
        if offset == 0:
            calculated_offsets = await self.fetch_one_and_calculate_offsets(params_dict)

        if not calculated_offsets:
            logger.debug("No locations found.")
            return None
        
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