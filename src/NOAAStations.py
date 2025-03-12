from typing import Optional

from request import Request


class NOAAStations(Request):
    """Class for fetching the available stations from the NOAA API."""
    def __init__(
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
        offset: Optional[int]=0,
        whitelist_path: Optional[str]=None,
        whitelist_key: Optional[str]=None,
        whitelist_value: Optional[str]=None) -> None:

        super().__init__(whitelist_path, whitelist_key, whitelist_value)

        self.datasetid = datasetid
        self.locationid = locationid
        self.datacategoryid = datacategoryid
        self.datatypeid = datatypeid
        self.extent = extent
        self.startdate = startdate
        self.enddate = enddate
        self.sortfield = sortfield
        self.sortorder = sortorder
        self.limit = limit
        self.offset = offset

        self.params_dict = {
            "datasetid": self.datasetid,
            "locationid": self.locationid,
            "datacategoryid": self.datacategoryid,
            "datatypeid": self.datatypeid,
            "extent": self.extent,
            "startdate": self.startdate,
            "enddate": self.enddate,
            "sortfield": self.sortfield,
            "sortorder": self.sortorder,
            "limit": self.limit,
            "offset": self.offset
        }
        self.q_string = self.build_query_string_from_dict(self.params_dict)
        self.data = None

    async def fetch(self) -> dict[str, str] | None:
        """Download the available stations according to the specified parameters.

        Returns:
            Optional[dict[str, str]]: A dictionary with 'metadata' and 'results' keys or None.
        """
        # Fetch station's endpoint
        data = await self.get("stations", self.q_string)

        self.data = data
        return data


if __name__ == "__main__":
    import asyncio

    async def main():
        noaa_stations = NOAAStations(datasetid='GSOM', locationid='FIPS:BR')
        stations = await noaa_stations.fetch()
        if stations:
            print(stations["metadata"])
            print(f"Total stations: {len(stations["results"])}")
    
    asyncio.run(main())