from datetime import datetime
from typing import Any, Optional
from loguru import logger

from NOAAStations import NOAAStations
from NOAALocations import NOAALocations
from request import Request
from utils.data import list_of_tuples_from_dict, save_to_csv
from utils.date import generate_year_date_range, is_more_than_10_years
from utils.log import format_log_content


class NOAAData(Request):
    def __init__(
        self,
        datasetid: str,
        startdate: str,
        enddate: str,
        blacklist_path: Optional[str]=None,
        WHITElist_path: Optional[str]=None,
        whitelist_key: Optional[str]=None,
        whitelist_value: Optional[str]=None,
        whitelist_title: Optional[str]=None,
        whitelist_description: Optional[str]=None) -> None:
        super().__init__("data", blacklist_path, WHITElist_path, whitelist_key, whitelist_value, whitelist_title, whitelist_description)

        self.datasetid = datasetid
        self.startdate = startdate
        self.enddate = enddate


    async def fetch_data(
        self,
        stationid: Optional[str]=None,
        datatypeid: Optional[str]=None,
        locationid: Optional[str]=None,
        units: Optional[str]=None,
        sortfield: Optional[str]=None,
        sortorder: Optional[str]=None,
        limit: int=1000,
        offsets: Optional[list[int]]=None,
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

        # Check if the whitelist is enabled and if the keys are present in the params_dict
        if self.whitelist:
            if self.whitelist_key not in params_dict.keys():
                raise ValueError("whitelist key should be in the query parameters")
            if self.whitelist_value not in params_dict.keys():
                raise ValueError("whitelist value should be in the query parameters")

        calculated_offsets = offsets

        # Check if the date range is more than 10 years
        # If so, split the date range into 10-year intervals
        if is_more_than_10_years(self.startdate, self.enddate):
            logger.warning("Fetching data for more than 10 years. This may take a while...")
            ten_year_ranges = generate_year_date_range(self.startdate, self.enddate, 10)
            
            data = {
                "metadata": {},
                "results": []
            }

            for start, end in ten_year_ranges:
                params_dict["startdate"] = start
                params_dict["enddate"] = end
                params_list = list_of_tuples_from_dict(params_dict, exclude_none=True)
                logger.info(format_log_content(context="Fetching data...", param_tuples=params_list, only_values=True))

                if offsets is None:
                    calculated_offsets = await self.fetch_one_and_calculate_offsets(params_dict)

                range_data = await self.get_with_offsets(params_dict, calculated_offsets)
                if range_data and "metadata" in range_data:
                    available = range_data["metadata"]["resultset"]["count"]
                    logger.success(format_log_content(context=f"Data found for range {start} to {end}", param_tuples=[("Returned items", f"{len(range_data["results"])}/{available}")]))
                    if not data["metadata"]:
                        data["metadata"] = range_data["metadata"]
                    data["results"].extend(range_data["results"])
                else:
                    logger.debug(f"No data found for range: {start} to {end}")
        else:
            params_list = list_of_tuples_from_dict(params_dict, exclude_none=True)
            logger.info(format_log_content(context="Fetching data...", param_tuples=params_list, only_values=True))

            if offsets is None:
                calculated_offsets = await self.fetch_one_and_calculate_offsets(params_dict)
            data = await self.get_with_offsets(params_dict, calculated_offsets)

        return data


    async def fetch_location_by_stations(
            self,
            locationid: str,
            startdate: Optional[str]=None,
            enddate: Optional[str]=None,
            verbose: Optional[bool]=0,
            save: bool=False) -> list[dict[str, Any]]:
        """Fetches data from a specific location using stations to
        avoid heavy loads in requests.

        It checks if the location is already in the whitelist, and
        if all its stations were already screened. If yes, it replaces
        the stations list provided by the one in the whitelist. Otherwise,
        it proceeds with the fecthing and whitelist updating operations.

        Args:
            locationid (str): The ID of the desired location.
            verbose (Optional[bool], default=False): A flag to enable verbose logging.

        Returns:
            list[dict[str, Any]]: A list of dictionaries containing station data.

        Raises:
            ValueError: If `startdate` is after `enddate`.
            FileNotFoundError: If the whitelist file is not found.
        """
        # Validate dates
        start = datetime.strptime(startdate or self.startdate, "%Y-%m-%d")
        end = datetime.strptime(enddate or self.enddate, "%Y-%m-%d")
        if start > end:
            raise ValueError("Start date must be before end date")

        stationsids = None

        # Try to retrieve whitelist for the given location (e.g., 'BR')
        whitelist = self.retrieve_whitelist(locationid)

        # If the location's whitelist is complete,
        # redefine'stationids' to include only the ones in the whitelist
        if whitelist and whitelist["metadata"]["status"] == "C":
            stationsids = whitelist[locationid]

            # This tells Request's 'get' method no item should be added to the whitelist for this location
            self.is_sub_whitelist_complete = True
        else:
            noaa_stations = NOAAStations()
            stations = await noaa_stations.fetch_stations(
                datasetid=self.datasetid,
                locationid=locationid,
            )

            if stations and "metadata" in stations:
                stationsids = [station["id"] for station in stations["results"]] \
                      if len(stations["results"]) > 1 else [stations["results"][0]["id"],]
            else:
                logger.debug(f"No stations found for location: {locationid}")
                return None

        complete_dataset = []  # Store all the data

        if stationsids:
            stations_count = 1  # Track whether all stations have been screened
            self.sub_whitelist_total_items = len(stationsids)
            for station_id in stationsids:
                try:
                    result = await self.fetch_data(stationid=station_id, locationid=locationid)
                    
                    if result:
                        data = result['results']
                        if save:
                            save_to_csv(data, f"data_{station_id}.csv")
                            logger.debug(f"Saved data to data_{station_id}.csv")
                        complete_dataset.extend(data)

                    stations_count += 1
                except Exception:
                    logger.exception(f"Failed to fetch data for station {station_id}")

            self.update_whitelist(locationid, "C")

            if verbose:
                log_content = format_log_content(
                    context="Location data" if complete_dataset else "Empty data",
                    param_tuples=[("Total items", len(complete_dataset)), ("Stations", len(stationsids), ("Successful requests", f"{self.success_count}/{self.requests_count}"))])
                if complete_dataset:
                    logger.success(log_content)
                else:
                    logger.debug(log_content)

        self.reset_whitelist()

        return complete_dataset


    async def fetch_locationcategory_by_stations(
        self,
        locationcategoryid: str,
        startdate: Optional[str]=None,
        enddate: Optional[str]=None,
        verbose: int=0,
        save: bool=False,
        cut_index: Optional[int]=None) -> list[dict[str, Any]]:
        """Fetch data by location category ID."""

        noaa_locations = NOAALocations()
        locations = await noaa_locations.fetch_locations(
            datasetid=datasetid,
            locationcategoryid=locationcategoryid,
            startdate=startdate or self.startdate,
            enddate=enddate or self.enddate
        )

        if locations:
            ids_names_dict = self.process_response_json(locations, "ids_names_dict")
            self.metadata = ids_names_dict

            # Ordered list of unique location IDs
            locations_list = np.unique([location["id"] for location in locations["results"]])
        else:
            logger.debug("No locations found")
        
        output_file = f"{datasetid}_{startdate}_{enddate}.csv"

        locations_list = locations_list[:cut_index] if cut_index else locations_list
        for locationid in locations_list:
            data = await self.fetch_location_by_stations(locationid=locationid)
            if data:
                if verbose:
                    logger.success(format_log_content(param_tuples=[
                        ("Country", ids_names_dict[locationid]),
                        ("Total items", len(data)),
                        ("Successful requests", f"{self.success_count}/{self.requests_count}")]))
                if save:
                    save_to_csv(data, output_file)
            else:
                logger.debug("Empty data")

        return data

if __name__ == "__main__":
    import asyncio
    import numpy as np

    WHITELIST_PATH = "whitelist.json"
    BLACKLIST_PATH = "blacklist.txt"

    datasetid = "GSOM"
    locationcategoryid = "CNTRY"
    startdate = "2000-01-01"
    enddate = "2025-04-14"

    loc_params = {
        "datasetid": datasetid,
        "locationcategoryid": locationcategoryid,
        "startdate": startdate,
        "enddate": enddate,
    }

    async def main():
        noaa_data = NOAAData(
            datasetid=datasetid,
            startdate=startdate,
            enddate=enddate,
            blacklist_path=BLACKLIST_PATH,
            WHITElist_path=WHITELIST_PATH,
            whitelist_key="locationid",
            whitelist_value="stationid",
            whitelist_title="CNTRY",
            whitelist_description="Stations' IDs and metadata for countries"
        )
        await noaa_data.fetch_locationcategory_by_stations(
            locationcategoryid=locationcategoryid,
            startdate=startdate,
            enddate=enddate,
            verbose=1,
            save=True,
            cut_index=2
        )
        noaa_data.save_blacklist()
        noaa_data.save_whitelist()

    asyncio.run(main())



            
