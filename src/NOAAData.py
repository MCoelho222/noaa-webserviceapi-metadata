from datetime import datetime
from typing import Any, Optional
from loguru import logger

from request import Request
from utils.data import list_of_tuples_from_dict
from utils.log import format_log_content
from NOAAStations import NOAAStations
from NOAALocations import NOAALocations


class NOAAData(Request):
    def __init__(
        self,
        datasetid: str,
        startdate: str,
        enddate: str,
        whitelist_path: Optional[str]=None,
        whitelist_key: Optional[str]=None,
        whitelist_value: Optional[str]=None) -> None:

        super().__init__("data", whitelist_path, whitelist_key, whitelist_value)

        self.datasetid = datasetid
        self.startdate = startdate
        self.enddate = enddate
        
        self.data = None


    async def fetch_data(
        self,
        stationid: Optional[str]=None,
        datatypeid: Optional[str]=None,
        locationid: Optional[str]=None,
        units: Optional[str]=None,
        sortfield: Optional[str]=None,
        sortorder: Optional[str]=None,
        limit: Optional[str]=1000,
        offsets: Optional[list[int]]=[0],
        includemetadata: Optional[bool]=True) -> dict[str, str] | None:
        """Fetch the data from the NOAA API.

        Returns:
            Optional[dict[str, str]]: A dictionary with 'metadata' and 'results' keys or None.
        """
        params_dict = {
            "datasetid": self.datasetid,
            "startdate": self.startdate,
            "enddate": self.enddate,
            "stationid": stationid,
            "datatypeid": datatypeid,
            "locationid": locationid,
            "units": units,
            "sortfield": sortfield,
            "sortorder": sortorder,
            "limit": limit,
            "includemetadata": includemetadata,
        }
        
        params_list = list_of_tuples_from_dict(params_dict, exclude_none=True)
        logger.info(format_log_content(context="Fetching data...", params=params_list))
        data = await self.get_with_offsets(params_dict, offsets)
        self.data = data
        return data


    async def fetch_location_data_by_stations(self, locationid: str, verbose: Optional[bool]=0) -> list[dict[str, Any]]:
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

        params = {
            "datasetid": self.datasetid,
            "startdate": self.startdate,
            "enddate": self.enddate,
            "locationid": locationid
        }

        stationsids = None

        # Try to retrieve whitelist for the given location (e.g., 'BR')
        loc_whitelist = self.retrieve_whitelist(locationid)

        # If the location's whitelist is complete,
        if loc_whitelist and loc_whitelist["metadata"] == "C":
            stationsids = loc_whitelist[locationid]
            self.set_is_whitelist_complete(True)
            # redefine'stationids' to include only the ones in the whitelist
        else:
            noaa_stations = NOAAStations()
            self.set_is_whitelist_complete(False)
            offsets = await noaa_stations.fetch_for_offsets(params)
            stations = await noaa_stations.fetch_stations(
                datasetid=self.datasetid,
                locationid=locationid,
                startdate=self.startdate,
                enddate=self.enddate,
                offsets=offsets
            )

            if stations and "metadata" in stations:
                stationsids = [station["id"] for station in stations["results"]] \
                      if len(stations["results"]) > 1 else [stations["results"][0]["id"],]

        complete_dataset = []  # Store all the data

        # Stations counter to track when all station have been sreened
        if stationsids:
            stations_count = 1
            for station_id in stationsids:
                try:
                    # Add station id to query string
                    if stations_count == len(stationsids) and not self.is_whitelist_complete:
                        self.set_is_whitelist_last_item(True)

                    result = await self.fetch_data(stationid=station_id, locationid=locationid)
                    
                    if result:
                        data = result['results']
                        complete_dataset.extend(data)

                    stations_count += 1  # Increase stations counter
                except Exception:
                    logger.exception(f"Failed to fetch data for station {station_id}")

            if verbose:
                log_content = format_log_content(
                    context="Data fetched" if complete_dataset else "Empty data",
                    params=[("Total items", len(complete_dataset)), ("Stations", len(stationsids))])
                if complete_dataset:
                    logger.success(log_content)
                else:
                    logger.debug(log_content)

        self.data = complete_dataset
        return complete_dataset


if __name__ == "__main__":
    import asyncio
    import numpy as np

    async def main(batch_init=0, batch_end=5):
        WHITELIST_PATH = "whitelist.json"
        datasetid = "GSOM"
        locationcategoryid = "CNTRY"
        startdate = "2020-01-01"
        enddate = "2025-03-07"

        noaa_data = NOAAData(
            datasetid=datasetid,
            startdate=startdate,
            enddate=enddate,
            whitelist_path=WHITELIST_PATH,
            whitelist_key="locationid",
            whitelist_value="stationid"
        )

        # Get all the available locations in the 'CNTRY' category within the specified time range
        loc_params = {
            "datasetid": datasetid,
            "locationcategoryid": locationcategoryid,
            "startdate": startdate,
            "enddate": enddate,
        }

        noaa_loc = NOAALocations()
        offsets = await noaa_loc.fetch_for_offsets(loc_params)
        locations = await noaa_loc.fetch_locations(
            datasetid=datasetid,
            locationcategoryid=locationcategoryid,
            startdate=startdate,
            enddate=enddate,
            offsets=offsets
        )

        if locations:
            ids_names_dict = noaa_loc.process_response_json(locations, "ids_names_dict")

            # Ordered list of unique location IDs
            locations_list = np.unique([location["id"] for location in locations["results"]])
        else:
            logger.debug("No locations found")

        for locationid in locations_list[batch_init:batch_end]:

            data = await noaa_data.fetch_location_data_by_stations(locationid=locationid)

            if data:
                logger.success(format_log_content(params=[("Country", ids_names_dict[locationid]), ("Total items", len(data))]))
            else:
                logger.debug("Empty data")

            noaa_data.save_whitelist()
    
    asyncio.run(main(batch_init=2, batch_end=3))



            
