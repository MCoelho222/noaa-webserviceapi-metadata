from datetime import datetime
from typing import Any, Optional
from loguru import logger

from request import Request
from utils.params import build_query_string
from utils.log import build_log_info


class NOAAData(Request):
    def __init__(
        self,
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
        whitelist_path: Optional[str]=None,
        whitelist_key: Optional[str]=None,
        whitelist_value: Optional[str]=None) -> None:

        super().__init__(whitelist_path, whitelist_key, whitelist_value)

        self.datasetid = datasetid
        self.startdate = startdate
        self.enddate = enddate
        self.stationid = stationid
        self.datatypeid = datatypeid
        self.locationid = locationid
        self.units = units
        self.sortfield = sortfield
        self.sortorder = sortorder
        self.limit = limit
        self.offset = offset
        self.includemetadata = includemetadata

        self.params_dict = {
            "datasetid": self.datasetid,
            "startdate": self.startdate,
            "enddate": self.enddate,
            "stationid": self.stationid,
            "datatypeid": self.datatypeid,
            "locationid": self.locationid,
            "units": self.units,
            "sortfield": self.sortfield,
            "sortorder": self.sortorder,
            "limit": self.limit,
            "offset": self.offset,
            "includemetadata": self.includemetadata,
        }
        self.q_string = build_query_string(self.params_dict)
        self.data = None


    async def fetch(self) -> dict[str, str] | None:
        """Fetch the data from the NOAA API.

        Returns:
            Optional[dict[str, str]]: A dictionary with 'metadata' and 'results' keys or None.
        """
        # Get all the available locations in the location category within the specified time range
        data = await self.get_request("data", build_query_string(self.params_dict))

        return data


    async def fetch_stations_by_location(self, stationsids: list[str], locationid: str, verbose: Optional[bool]=0) -> list[dict[str, Any]]:
        """Fetches station data for a specific country within a date range.

        This function retrieves station data from an external source for a given dataset,
        country location, a list of station IDs, and time range. It checks if the location is
        already in the whitelist, and if all its stations were already screened. If yes,
        it replaces the stations list provided by the one in the whitelist. Otherwise, it proceeds
        with the fecthing and whitelist updating operations. Additionally, if 'step_months' is
        provided, the time range is split into intervals, to avoid reaching the items max-limit per request.

        Args:
            verbose (Optional[bool], default=False): A flag to enable verbose logging.

        Returns:
            list[dict[str, Any]]: A list of dictionaries containing station data.

        Raises:
            ValueError: If `startdate` is after `enddate`.
            FileNotFoundError: If the whitelist file is not found.
        """
        # Validate dates
        start = datetime.strptime(self.startdate, "%Y-%m-%d")
        end = datetime.strptime(self.enddate, "%Y-%m-%d")
        if start > end:
            raise ValueError("Start date must be before end date")

        # Try to retrieve whitelist for the given location (e.g., 'BR')
        whitelist = self.retrieve_whitelist(locationid)

        # If the location's whitelist is complete,
        if whitelist:
            is_complete = whitelist["metadata"] == "C"
            if is_complete:
                # redefine 'stationids' to include only the ones in the whitelist
                stationsids = [station_id for station_id in stationsids if station_id in whitelist[locationid]]

        complete_dataset = []  # Store all the data

        # Stations counter to track when all station have been sreened
        stations_count = 0

        self.set_is_whitelist_complete(False)
        for station_id in stationsids:
            try:
                # Add station id to query string
                q_string = self.q_string + f"&locationid={locationid}&stationid={station_id}"

                if stations_count == len(stationsids) - 1:
                    is_complete = self.set_is_whitelist_complete(True)

                result = await self.get_request('data', q_string)
                
                data = None

                if result:
                    data = result['results']
                    complete_dataset.extend(data)

                stations_count += 1  # Increase stations counter
            except Exception:
                logger.exception(f"Failed to fetch data for station {station_id}")

        if verbose:
            log_content = build_log_info(
                context="Data fetched" if complete_dataset else "Empty data",
                params=[("Total items", len(complete_dataset)), ("Stations", len(stationsids)), ("Whitelist", is_complete)])
            if complete_dataset:
                logger.success(log_content)
            else:
                logger.debug(log_content)

        self.data = complete_dataset
        return complete_dataset

if __name__ == "__main__":
    import asyncio
    import numpy as np

    from noaa_locations import NOAALocations
    from noaa_stations import NOAAStations
    from utils.params import calculate_offsets

    async def main():
        WHITELIST_PATH = "whitelist.json"
        datasetid = "GSOM"
        locationcategoryid = "CNTRY"
        startdate = "2020-01-01"
        enddate = "2025-03-07"
        limit = 1000

        noaa_data = NOAAData(
            datasetid=datasetid,
            startdate=startdate,
            enddate=enddate,
            whitelist_path=WHITELIST_PATH,
            whitelist_key="locationid",
            whitelist_value="stationid"
        )

        # Get all the available locations in the 'CNTRY' category within the specified time range
        noaa_locations = NOAALocations(
            datasetid=datasetid,
            locationcategoryid=locationcategoryid,
            startdate=startdate,
            enddate=enddate,
            limit=limit
        )

        locations = await noaa_locations.fetch()

        if locations:
            ids_names_dict = noaa_locations.process_response("ids_names_dict")

            # Ordered list of unique location IDs
            locations_list = np.unique([location["id"] for location in locations["results"]])

        else:
            logger.debug("No locations found")

        locations_batch = locations_list[0:5]  # Start with the first 5 locations

        for locationid in locations_batch:
            # Retrieve the whitelist for the current location
            whitelist = noaa_data.retrieve_whitelist(locationid)

            offsets = [0]  # Initial offsets (0 fetches all)

            # If there's no whitelist for this location,
            # or despite existing it's incomplete, fetch for available stations
            if not whitelist or (whitelist and whitelist["metadata"] == "I"):
                # Make a initial fetch and check the metadata to find out the number of available stations
                noaa_stations = NOAAStations(datasetid=datasetid, locationid=locationid)
                stations = await noaa_stations.fetch()

                if stations:
                    stations_metadata = stations["metadata"]

                    count = int(stations_metadata["resultset"]["count"])

                # If there are more than 1000, calculate the offsets for fetching stations in smaller steps
                if count > 1000:
                    offsets = calculate_offsets(count)

                    log_content = build_log_info(
                        context="Offsets calculated",
                        params=[
                            ("Stations", count),
                            ("Offsets", len(offsets))
                            ]
                        )
                    logger.info(log_content)

                # Fetch available station using offsets
                stations = []
                for offset in offsets:
                    noaa_stations.offset = offset
                    station_ids = await noaa_stations.fetch()
                    if station_ids and "results" in station_ids.keys():
                        stations.extend([station['id'] for station in station_ids["results"]])

                unique_stations_ids = np.unique(stations)  # Ordered list of unique stations

            elif whitelist and whitelist["metadata"] == "C":
                unique_stations_ids = whitelist[locationid]

            # Fetch data from stations
            if len(unique_stations_ids) > 0:
                data = await noaa_data.fetch_stations_by_location(locationid=locationid, stationsids=unique_stations_ids)

                if data:
                    log_content = build_log_info(
                        params=[
                            ("Country", ids_names_dict[locationid]),
                                ("Stations count", len(unique_stations_ids)),
                                ("Total rows", len(data))
                                ]
                        )
                    logger.success(log_content)
                else:
                    logger.debug("Empty data")
    
    asyncio.run(main())



            
