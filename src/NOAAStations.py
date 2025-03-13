from typing import Optional

from request import Request


class NOAAStations():
    """Class for fetching the available stations from the NOAA API."""
    def __init__(self) -> None:
        self.data = None

    async def fetch_stations(
        self,
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
        offsets: Optional[list[int]]=[0]) -> dict[str, str] | None:
        """Download the available stations according to the specified parameters.

        Returns:
            Optional[dict[str, str]]: A dictionary with 'metadata' and 'results' keys or None.
        """
        params_dict = {
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
        }

        req = Request()
        data = await req.get_with_offsets("stations", params_dict, offsets)
        return data


if __name__ == "__main__":
    import asyncio

    async def main():
        noaa_stations = NOAAStations()
        stations = await noaa_stations.fetch_stations(datasetid='GSOM', locationid='FIPS:BR')
        if stations:
            print(stations["metadata"])
            print(f"Total stations downloaded: {len(stations["results"])}")
    
    asyncio.run(main())