import json
import os

from datetime import datetime, timezone
from dotenv import load_dotenv
from loguru import logger
from typing import Optional

from utils.data import parse_size, parse_size_to_human_read
from utils.log import format_log_content

load_dotenv()


class Whitelist:
    """
    A class that manages a whitelist with this structure: 

        {
            "metadata": {
                "FIPS:BR": "C",
                "FIPS:US": "I"
            },
            "FIPS:BR": [abs123, abc456, ...],
            "FIPS:US": [abs123, abc456, ...]
        }

    This whitelist is supposed to store query parameters that return actual data (non-empty)
    when fetching the NOAA Web Services API. In this example, the target key is the 'locationid'
    from the URL being fetched (e.g., 'FIPS:BR') and the values come from the 'stationid' query
    parameter. The 'metadata' key contains the status of each key's whitelist. "C" stands for
    "complete" and "I" for "incomplete". A key's whitelist is complete when all the available
    values related to a key have been fetched and verified regarding their data content.
    """
    def __init__(self, wl_path: str, wl_target: Optional[str]=None, wl_description: Optional[str]=None) -> None:
        """Creates an instance of a whitelist.

        Args:
            wl_path (Optional[str], optional): The path to retrieve an existent whitelist or to save a new one. Defaults to None.
            whitelist_key (Optional[str], optional): One of the query parameters from the URL to be used as the key that holds a whitelist,
                e.g., 'stationid' or 'locationid'. Defaults to None.
            whitelist_value (Optional[str], optional): One of the query parameters from the URL to be used as the values inside a whitelist,
                e.g., 'stationid' or 'locationid'. Defaults to None.
        """
        self.wl_path = wl_path
        self.wl_target = wl_target
        self.wl_description = wl_description

        self.whitelist = self._create_or_load_whitelist()

        # Attributes to manage the whitelist from child classes
        self.is_sub_whitelist_complete = False
        self.sub_whitelist_total_items = 0
        

    def _create_or_load_whitelist(self) -> dict[str, list[str] | dict[str, str]]:
        """Creates a whitelist or loads an existent one.

        Returns:
            dict: The whitelist JSON file.
        """
        if os.path.exists(self.wl_path):
            try:
                context = "Whitelist loaded"
                with open(self.wl_path, "r") as f:
                    logger.info(context)
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                logger.error("Whitelist could not be loaded.")
                return None

        context = "Whitelist created"
        whitelist = {
            "target": self.wl_target if self.wl_target else "",
            "description": self.wl_description if self.wl_description else "",
            "metadata": {
                "created": datetime.now(timezone.utc).isoformat(),
                "updated": datetime.now(timezone.utc).isoformat(),
                "total_items": 0,
                "total_size": "0 B"
            },
        }

        return whitelist
            

    def add_to_whitelist(self, key: str, value: str, metadata: dict[str, str | int]) -> None:
        """Includes an item in the whitelist.

        Args:
            key (str): The whitelist key where the value should be included (e.g., 'FIPS:BR')
            value (str): The value to be included in the given whitelist key.
        """
        info = {"items": metadata["items"], "size": parse_size_to_human_read(metadata["size"])}
        try:
            log_params = [("Key", key), ("Value", value)]

            # When the key already exists in the whitelist and the value is new
            if key in self.whitelist.keys():
                self.whitelist[key][value] = {"items": metadata["items"], "size": parse_size_to_human_read(metadata["size"])}
                self.whitelist["metadata"][key]["count"] = f"{len(self.whitelist[key])}/{self.sub_whitelist_total_items}"
                self.whitelist["metadata"][key]["size"] = parse_size_to_human_read(
                    parse_size(self.whitelist["metadata"][key]["size"]) + metadata["size"])
                self.whitelist["metadata"][key]["items"] = self.whitelist["metadata"][key]["items"] + metadata["items"]


            # When the item is the first one from a set of items that will be added to the whitelist
            # and the whitelist is not complete yet, it is marked as incomplete
            else:
                self.whitelist["metadata"][key] = {
                    **metadata,
                    "status": "Incomplete",
                    "count": f"1/{self.sub_whitelist_total_items}",
                    "size": parse_size_to_human_read(metadata["size"]),
                    "items": metadata["items"]
                }
                self.whitelist[key] = {value: info}

            self.whitelist["metadata"]["total_items"] = self.whitelist["metadata"]["total_items"] + metadata["items"]
            self.whitelist["metadata"]["total_size"] = parse_size_to_human_read(parse_size(self.whitelist["metadata"]["total_size"]) + metadata["size"])
            self.whitelist["metadata"]["updated"] = datetime.now(timezone.utc).isoformat()
        except Exception:
            logger.exception(format_log_content(context="Failed adding to whitelist", param_tuples=log_params))


    def retrieve_whitelist(self, target_key: Optional[str] = None) -> dict[str, list[str] | dict[str, str]]:
        """Retrives a specified whitelist or the complete whitelist.

        Args:
            target_key (str): The target key to retrieve in the whitelist

        Returns:
            dict[str, list[str] | dict[str, str]] | dict: A dictionary with
                the metadata and the whitelist for a given target key; The complete
                whitelist if 'target_key' is not specified; An empty dictionary if
                the 'target_key' doesn't exist in the whitelist.
        """
        if not target_key:
            return self.whitelist

        if target_key in self.whitelist.keys():
            logger.info("whitelist retrieved")
            return {
                "metadata": self.whitelist["metadata"][target_key],
                target_key: list(self.whitelist[target_key].keys())
            }
        else:
            return {}


    def reset_whitelist(self):
        self.is_sub_whitelist_complete = False
        self.is_whitelist_last_item = False
        self.sub_whitelist_total_items = 0


    def save_whitelist(self) -> None:
        """Saves the whitelist."""
        if self.whitelist:
            try:
                os.makedirs(os.path.dirname(self.wl_path), exist_ok=True)
                with open(self.wl_path, "w") as f:
                    json.dump(self.whitelist, f, indent=4)
                logger.success(f"Whitelist saved to {self.wl_path}")
            except FileNotFoundError:
                logger.error(f"File not found: {self.wl_path}")
        else:
            logger.debug("No whitelist to be saved")


    def update_whitelist(self, key: str, status: str) -> None:
        """Updates the status of a given whitelist key.

        Args:
            key (str): The key to be updated in the whitelist.
            status (str): The new status of the whitelist key.
        """
        if key in self.whitelist["metadata"].keys():
            self.whitelist["metadata"][key]["status"] = status
            logger.success(f"Whitelist complete: {key}")
        else:
            logger.error(f"whitelist {key} not found")

if __name__ == "__main__":
    # Initialize the whitelist with a file path
    whitelist = Whitelist(wl_path="whitelist_test.json")

    # Add items to the whitelist
    whitelist.add_to_whitelist("FIPS:BR", "GHCND:BR000352000")
    whitelist.add_to_whitelist("FIPS:US", "GHCND:USW00094728")

    # Retrieve a specific whitelist entry
    br_whitelist = whitelist.retrieve_whitelist("FIPS:BR")
    print("BR whitelist:", br_whitelist)

    # Retrieve the full whitelist
    full_whitelist = whitelist.retrieve_whitelist()
    print("Full whitelist:", full_whitelist)

    # Save the whitelist to file
    whitelist.save_whitelist()