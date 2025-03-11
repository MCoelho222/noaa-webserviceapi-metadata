from typing import Optional

from request import Request
from utils.params import build_query_string


class NOAALocations(Request):
    """Class for fetching the available locations from the NOAA API."""
    def __init__(
        self,
        datasetid: Optional[str]=None,
        locationcategoryid: Optional[str]=None,
        datacategoryid: Optional[str]=None,
        startdate: Optional[str]=None,
        enddate: Optional[str]=None,
        sortfield: Optional[str]=None,
        sortorder: Optional[str]=None,
        limit: Optional[str]=1000,
        offset: Optional[str]=0,
        whitelist_path: Optional[str]=None,
        whitelist_key: Optional[str]=None,
        whitelist_value: Optional[str]=None) -> None:

        super().__init__(whitelist_path, whitelist_key, whitelist_value)

        self.datasetid = datasetid
        self.locationcategoryid = locationcategoryid
        self.datacategoryid = datacategoryid
        self.startdate = startdate
        self.enddate = enddate
        self.sortfield = sortfield
        self.sortorder = sortorder
        self.limit = limit
        self.offset = offset

        self.params_dict = {
            "datasetid": self.datasetid,
            "locationcategoryid": self.locationcategoryid,
            "datacategoryid": self.datacategoryid,
            "startdate": self.startdate,
            "enddate": self.enddate,
            "sortfield": self.sortfield,
            "sortorder": self.sortorder,
            "limit": self.limit,
            "offset": self.offset
        }

        self.data = None

    async def fetch(self,) -> dict[str, str]:
        """Fetch the locations from the NOAA API.

        Returns:
            dict: The locations fetched from the NOAA API.
        """
        # Get all the available locations
        data = await self.get_request("locations", build_query_string(self.params_dict))

        self.data = data
        return data


if __name__ == "__main__":
    import asyncio

    async def main():
        noaa_locations = NOAALocations(datasetid='GSOM', locationcategoryid='CITY')
        locations = await noaa_locations.fetch()
        if locations:
            print(locations["metadata"])
            print(locations["results"][:5])
    
    asyncio.run(main())