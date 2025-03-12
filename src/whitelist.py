import json
import os
from dotenv import load_dotenv
from loguru import logger
from typing import Optional

from utils.log import formatted_log_content

load_dotenv()


class Whitelist():
    def __init__(self, whitelist_path: Optional[str]=None, whitelist_key: Optional[str]=None, whitelist_value: Optional[str]=None):
        self.whitelist_path = whitelist_path
        self.whitelist_key = whitelist_key
        self.whitelist_value = whitelist_value
        self.is_whitelist_complete = False

        self.whitelist = self.set_whitelist()

    def set_whitelist(self) -> dict[str, str]:
        """Set the whitelist JSON file.

        Returns:
            dict: The whitelist JSON file.
        """
        try:
            # Create the whitelist if it doesn't exist
            if not self.whitelist_path or not os.path.exists(self.whitelist_path):
                logger.info(formatted_log_content(context="Whitelist started", params=[("whitelist_path", self.whitelist_path),]))
                return {
                    "metadata": {},
                }  
            else:
                # Load the whitelist JSON
                with open(self.whitelist_path, "r") as f:
                    whitelist = json.load(f)
                logger.info(formatted_log_content(context="Whitelist loaded", params=[("whitelist_path", self.whitelist_path),]))
                return whitelist
        except Exception:
            logger.exception(msg="Failed setting whitelist")


    def set_whitelist_key(self, key: str):
        self.whitelist_key = key


    def set_whitelist_value(self, value: str):
        self.whitelist_value = value


    def set_is_whitelist_complete(self, is_whitelist_complete: bool):
        self.is_whitelist_complete = is_whitelist_complete


    def is_whitelist_ready(self, params: list[tuple[str, str]]) -> bool:
        """Check if the whitelist is ready to be used.

        Args:
            params (list[tuple[str, str]]): The query parameters.

        Returns:
            bool: True if the whitelist is ready, False otherwise.
        """
        if not self.whitelist_path:
            logger.debug("Whitelist path is missing")
            return False

        # Ensure whitelist_key and whitelist_value are in the query parameters
        if (self.whitelist_path and not self.whitelist_key) or (self.whitelist_path and not self.whitelist_value):
            logger.error("Both whitelist_key and whitelist_value must be provided")
            return False

        if (self.whitelist_key and not self.whitelist_value) or (self.whitelist_value and not self.whitelist_key):
            logger.error("Both whitelist_key and whitelist_value must be provided")
            return False

        if self.whitelist_key and self.whitelist_key not in [param[0] for param in params]:
            logger.error("Missing whitelist_key in the query parameters")
            return False
        
        if self.whitelist_value and self.whitelist_value not in [param[0] for param in params]:
            logger.error("Missing whitelist_value in the query parameters")
            return False

        return True


    def add_to_whitelist(self, key: str, value: Optional[str]=None, is_whitelist_complete: bool=False) -> None:
        """Includes a target feature in the whitelist.

        The whitelist has the following example structure: 

        {
            "metadata": {
                "FIPS:BR": "C",
                "FIPS:US": "I"
            },
            "FIPS:BR": [abs123, abc456, ...],
            "FIPS:US": [abs123, abc456, ...]
        }

        In this example, the keys are the locations (e.g., 'FIPS:BR') and the values are station IDs.
        The metadata key contains the status of each location's whitelist.
        "C" stands for "complete" and "I" for "incomplete". A location is complete when all
        the available stations have been fetched and the ones with actual data are included in the whitelist.

        Args:
            key (str): The target feature to be used as key (e.g., 'FIPS:BR')
            value (str, optional): The target feature to be used as value. If None, the whitelist for the target key is considered complete.
            is_whitelist_complete (bool, optional): If True, the location is considered complete.
        """
        try:
            log_params = [("Key", key), ("Value", value), ("is_whitelist_complete", is_whitelist_complete)]

            # Check if the location exists in the whitelist
            if key in self.whitelist.keys():
                if value is None:  # Location is complete, update metadata
                    self.whitelist["metadata"][key] = "C"
                    self.update_whitelist()
                    logger.success(formatted_log_content(context="Complete", params=log_params))

                # Check if the station ID is already included in the location's whitelist
                elif value not in self.whitelist[key]:
                    # Include in the whitelist
                    self.whitelist[key].append(value)

                    if is_whitelist_complete:
                        self.whitelist["metadata"][key] = "C"
                        logger.success(formatted_log_content(context="Complete", params=log_params))
                    else:
                        logger.info(formatted_log_content(context="Appended", params=log_params))
                    self.update_whitelist()

                elif value in self.whitelist[key]:  # Just log if it's already included
                    if is_whitelist_complete:
                        self.whitelist["metadata"][key] = "C"
                        logger.warning(formatted_log_content(msg="Already in whitelist, location complete", params=log_params))
                    else:
                        logger.warning(formatted_log_content(msg="Already in whitelist", params=log_params))

            else:  # Create key if it doesn't exist
                self.whitelist["metadata"][key] = "I" if not is_whitelist_complete else "C"
                self.whitelist[key] = [value,]

                logger.info(formatted_log_content(context="Appended", params=log_params))
                self.update_whitelist()
        except Exception:
            logger.exception(formatted_log_content(context="Failed adding to whitelist", params=log_params))


    def retrieve_whitelist(self, target_key: Optional[str] = None) -> dict[str, str]:
        """Retrive the whitelist for a given target key or the complete whitelist.

        Args:
            target_key (str): The target key to retrieve in the whitelist

        Returns:
            dict[str, str]: A dictionary with the metadata and stations for the given key,
                or empty if the key doesn't exist. Also, returns the complete whitelist if no key is provided.

        Raises:
            FileNotFoundError: If the path doesn't exist.
        """
        if not target_key:
            return self.whitelist  # Return the complete whitelist

        # If 'target_key' was given and is a whitelist key, return metadata and whitelist
        if target_key in self.whitelist.keys():
            target_whitelist = [item.replace('\n', '') for item in self.whitelist[target_key]]
            return {
                "metadata": self.whitelist["metadata"][target_key],
                target_key: target_whitelist
            }
        else:
            logger.warning(f"Key '{target_key}' not found in the whitelist")
            return {}


    def update_whitelist(self):
        """Update the whitelist file with the updated JSON."""
        # Update the whitelist file with the updated JSON
        with open(self.whitelist_path, "w") as f:
            json.dump(self.whitelist, f, indent=4)
        logger.info("Whitelist JSON updated")


if __name__ == "__main__":
    # import asyncio

    params = {
        "datasetid": "GSOM",
        "locationid": "FIPS:BR",
        "stationid": "GHCND:BR000352000",
        "startdate": "2020-01-01",
        "enddate": "2024-12-31",
        "limit": 1000
    }

    # async def main():
    #     whitelist = WhitelistFetchNOAA(whitelist_path="whitelist_test.json")
    #     result = await whitelist.fetch('data', q_string, "locationid", "stationid", False)
    #     return result
    
    # asyncio.run(main())