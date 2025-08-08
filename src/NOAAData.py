import json
from typing import Any, Optional
from loguru import logger

from NOAAStations import NOAAStations
from NOAALocations import NOAALocations
from request import Request
from utils.data import list_of_tuples_from_dict, save_to_csv
from utils.date import generate_year_date_range, is_more_than_10_years
from utils.log import format_log_content
from blacklist import Blacklist
from whitelist import Whitelist


class NOAAData(Request, Blacklist):
    def __init__(self, datasetid: str, startdate: str, enddate: str, blacklist_path: Optional[str]=None) -> None:
        super().__init__("data")
        Blacklist.__init__(self, blacklist_path=blacklist_path)
        
        self.datasetid = datasetid
        self.startdate = startdate
        self.enddate = enddate


    async def fetch_data(
        self,
        stationid: Optional[str]=None,
        datatypeid: Optional[str]=None,
        locationid: Optional[str]=None,
        startdate: Optional[str]=None,
        enddate: Optional[str]=None,
        units: Optional[str]=None,
        sortfield: Optional[str]=None,
        sortorder: Optional[str]=None,
        limit: int=1000,
        offset: int=0,
        includemetadata: Optional[bool]=True) -> dict[str, str] | None:
        """Fetch the data from the NOAA API.

        Returns:
            Optional[dict[str, str]]: A dictionary with 'metadata' and 'results' keys or None.
        """
        params_dict = {
            "datasetid": self.datasetid,
            "startdate": startdate if startdate else self.startdate,
            "enddate": enddate if enddate else self.enddate,
            "stationid": stationid,
            "datatypeid": datatypeid,
            "locationid": locationid,
            "units": units,
            "sortfield": sortfield,
            "sortorder": sortorder,
            "limit": limit,
            "includemetadata": includemetadata,
        }

        calculated_offsets = [offset]

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
                if self.is_blacklisted(self.build_query_string_from_dict(params_dict)):
                    logger.debug(format_log_content(context="Blacklisted. Skipping...", param_tuples=params_list, only_values=True))
                    continue
                
                logger.info(format_log_content(context="Fetching data...", param_tuples=params_list, only_values=True))

                if offset == 0:
                    calculated_offsets = await self.fetch_one_and_calculate_offsets(params_dict)
                    if calculated_offsets is None:
                        logger.debug(f"No data found for range: {start} to {end}")
                        self.add_to_blacklist(self.build_query_string_from_dict(params_dict))
                        continue

                range_data = await self.get_with_offsets(params_dict, calculated_offsets)
                if range_data is None:
                    logger.debug(f"No data found for range: {start} to {end}")
                    self.add_to_blacklist(self.build_query_string_from_dict(params_dict))
                    continue

                if "metadata" in range_data.keys():
                    available = range_data["metadata"]["resultset"]["count"]
                    logger.success(
                        format_log_content(
                            context=f"Data found for range {start} to {end}",
                            param_tuples=[("Returned items", f"{len(range_data["results"])}/{available}")]))

                    if not data["metadata"]:
                        data["metadata"] = range_data["metadata"]

                    data["results"].extend(range_data["results"])
        else:
            params_list = list_of_tuples_from_dict(params_dict, exclude_none=True)
            if self.is_blacklisted(self.build_query_string_from_dict(params_dict)):
                logger.debug(format_log_content(context="Blacklisted URL. Skipping...", param_tuples=params_list, only_values=True))
                return None
            logger.info(format_log_content(context="Fetching data...", param_tuples=params_list, only_values=True))

            if offset == 0:
                calculated_offsets = await self.fetch_one_and_calculate_offsets(params_dict)
                if calculated_offsets is None:
                    logger.debug(f"No data found for range: {startdate} to {enddate}")
                    self.add_to_blacklist(self.build_query_string_from_dict(params_dict))
                    return None
            
            data = await self.get_with_offsets(params_dict, calculated_offsets)
            if not data:
                logger.debug("I WAS USED")
                self.add_to_blacklist(self.build_query_string_from_dict(params_dict))
                return None
        return data


    async def fetch_location_by_stations(
        self,
        locationid: str,
        startdate: Optional[str]=None,
        enddate: Optional[str]=None,
        verbose: Optional[bool]=0,
        save: bool=False,
        use_whitelist: bool=True,
        wl_target: str="locationcategoryid",
        wl_description: str="CNTRY") -> list[dict[str, Any]]:
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
        stationsids = None

        if use_whitelist:
            wl_path = f"whitelist/{wl_description}_whitelist.json"
            wl = Whitelist(wl_path, wl_target, wl_description)

            # Try to retrieve whitelist for the given location (e.g., 'BR')
            whitelist = wl.retrieve_whitelist(locationid)

            # If the location's whitelist is complete,
            # redefine'stationids' to include only the ones in the whitelist
            if whitelist and whitelist["metadata"]["status"] == "Complete":
                wl.is_sub_whitelist_complete = True
                stationsids = whitelist[locationid]

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
            total_items = len(stationsids)
            for station_id in stationsids:
                try:
                    data = await self.fetch_data(stationid=station_id, locationid=locationid, startdate=startdate, enddate=enddate)

                    if data and data['results']:
                        results = data['results']

                        # The whitelist is used for the 'data' endpoint only
                        if use_whitelist and not wl.is_sub_whitelist_complete:
                            wl.sub_whitelist_total_items = total_items
                            size_bytes = len(json.dumps(results).encode("utf-8"))  # Convert JSON to bytes
                            wl.add_to_whitelist(
                                key=locationid,
                                value=station_id,
                                metadata={
                                    "name": self.metadata[locationid],
                                    "items": len(results),
                                    "size": size_bytes
                                }
                            )
                        if save:
                            save_to_csv(results, f"data_{station_id}.csv")
                            logger.debug(f"Saved data to data_{station_id}.csv")
                        complete_dataset.extend(results)
                except Exception:
                    logger.exception(f"Failed to fetch data for station {station_id}")

            if use_whitelist and not wl.is_sub_whitelist_complete:
                wl.update_whitelist(locationid, "Complete")
                wl.save_whitelist()

            if verbose:
                log_content = format_log_content(
                    context="Location data" if complete_dataset else "Empty data",
                    param_tuples=[("Total items", len(complete_dataset)), ("Stations", len(stationsids), ("Successful requests", f"{self.success_count}/{self.requests_count}"))])
                if complete_dataset:
                    logger.success(log_content)
                else:
                    logger.debug(log_content)

        self.save_blacklist()
        return complete_dataset


    async def fetch_locationcategory_by_stations(
        self,
        locationcategoryid: str,
        startdate: Optional[str]=None,
        enddate: Optional[str]=None,
        verbose: int=0,
        use_whitelist: bool=True,
        wl_target: str="locationcategoryid",
        wl_description: str="CNTRY",
        save: bool=False,
        cut_index: Optional[int]=None
        ) -> list[dict[str, Any]]:
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
            data = await self.fetch_location_by_stations(
                locationid=locationid,
                use_whitelist=use_whitelist,
                wl_target=wl_target,
                wl_description=wl_description
            )
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
        )
        await noaa_data.fetch_locationcategory_by_stations(
            locationcategoryid=locationcategoryid,
            startdate=startdate,
            enddate=enddate,
            use_whitelist=True,
            verbose=1,
            save=True,
            cut_index=3
        )

    asyncio.run(main())



            
