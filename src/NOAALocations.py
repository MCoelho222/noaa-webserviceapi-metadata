from typing import Optional

from request import Request


class NOAALocations():
    """Class for fetching the available locations from the NOAA API."""
    def __init__(self) -> None:
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
        limit: Optional[str]=1000,
        offsets: Optional[list[int]]=[0]) -> dict[str, str]:
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
        req = Request()
        data = await req.get_with_offsets("locations", params_dict, offsets)
        return data


if __name__ == "__main__":
    import asyncio

    async def main():
        noaa_locations = NOAALocations()
        locations = await noaa_locations.fetch_locations(datasetid='GSOM', locationcategoryid='CITY')
        if locations:
            print(locations["metadata"])
            print(locations["results"][:5])
    
    asyncio.run(main())