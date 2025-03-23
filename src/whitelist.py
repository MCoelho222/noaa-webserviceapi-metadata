import json
import os

from datetime import datetime, timezone
from dotenv import load_dotenv
from loguru import logger
from typing import Optional

from utils.data import parse_size, parse_size_to_human_read
from utils.log import format_log_content

load_dotenv()


class Whitelist():
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
    def __init__(
        self,
        whitelist_path: Optional[str]=None,
        whitelist_key: Optional[str]=None,
        whitelist_value: Optional[str]=None,
        whitelist_title: Optional[str]=None,
        whitelist_description: Optional[str]=None) -> None:
        """Creates an instance of a Whitelist.

        Args:
            whitelist_path (Optional[str], optional): The path to retrieve an existent whitelist or to save a new one. Defaults to None.
            whitelist_key (Optional[str], optional): One of the query parameters from the URL to be used as the key that holds a whitelist,
                e.g., 'stationid' or 'locationid'. Defaults to None.
            whitelist_value (Optional[str], optional): One of the query parameters from the URL to be used as the values inside a whitelist,
                e.g., 'stationid' or 'locationid'. Defaults to None.
        """
        self.whitelist_path = whitelist_path
        self.whitelist_key = whitelist_key
        self.whitelist_value = whitelist_value
        self.whitelist_title = whitelist_title
        self.whitelist_description = whitelist_description
        self.whitelist = self._create_or_load_whitelist()

        # Attributes to manage the whitelist from child classes
        self.is_sub_whitelist_complete = False
        self.is_whitelist_last_item = False  # Used to track whether a new item is the last one from a whitelist
        self.sub_whitelist_total_items = 0
        

    def _create_or_load_whitelist(self) -> dict[str, list[str] | dict[str, str]]:
        """Creates a whitelist or loads an existent one.

        Returns:
            dict: The whitelist JSON file.
        """
        if self._is_whitelist_ready():
            log_params = [("Path", self.whitelist_path)]
            try:
                if not os.path.exists(self.whitelist_path):
                    context = "Whitelist created"
                    whitelist = {
                        "title": self.whitelist_title if self.whitelist_title else "",
                        "description": self.whitelist_description if self.whitelist_description else "",
                        "metadata": {
                            "created": datetime.now(timezone.utc).isoformat(),
                            "updated": datetime.now(timezone.utc).isoformat(),
                            "total_items": 0,
                            "total_size": "0 B"
                        },
                    }
                else:
                    try:
                        context = "Whitelist loaded"
                        with open(self.whitelist_path, "r") as f:
                            whitelist = json.load(f)
                    except (json.JSONDecodeError, FileNotFoundError):
                        whitelist = {
                            "metadata": {
                                "created": datetime.now(timezone.utc).isoformat(),
                                "updated": datetime.now(timezone.utc).isoformat(),
                                "total_items": 0,
                                "total_size": "0 B"
                            },
                        }
                        logger.error("The whitelist could not be loaded. A new one was created.")
                logger.info(format_log_content(context=context, params=log_params))
                return whitelist
            except Exception:
                logger.exception(msg="Whitelist could not be loaded or created")
        else:
            return {}


    def _is_whitelist_ready(self) -> bool:
        """Checks if the whitelist is ready to be used.

        Is a whitelist path defined?
        Are both a key and value defined?
        Are both key and value defined in the URL query parameters?

        Args:
            params (list[tuple[str, str]]): The query parameters.

        Returns:
            bool: True if the whitelist is ready, False otherwise.
        """
        if not self.whitelist_path:
            return False
        elif not self.whitelist_key or not self.whitelist_value:
            logger.error("Both 'whitelist_key' and 'whitelist_value' must be provided")
            return False

        return True


    def add_to_whitelist(self, key: str, value: str, metadata: dict[str, str | int]) -> None:
        """Includes an item in the whitelist.

        Args:
            key (str): The whitelist key where the value should be included (e.g., 'FIPS:BR')
            value (str): The value to be included in the given whitelist key.
        """
        info = {"items": metadata["items"], "size": parse_size_to_human_read(metadata["size"])}
        try:
            log_params = [("Key", key), ("Value", value)]
            if key in self.whitelist.keys():
                self.whitelist[key][value] = {"items": metadata["items"], "size": parse_size_to_human_read(metadata["size"])}
                self.whitelist["metadata"][key]["count"] = f"{len(self.whitelist[key])}/{self.sub_whitelist_total_items}"
                self.whitelist["metadata"][key]["size"] = parse_size_to_human_read(
                    parse_size(self.whitelist["metadata"][key]["size"]) + metadata["size"])
                self.whitelist["metadata"][key]["items"] = self.whitelist["metadata"][key]["items"] + metadata["items"]
    
                if self.is_whitelist_last_item:
                    self.whitelist["metadata"][key]["status"] = "C"

                    logger.success(format_log_content(context="Whitelist complete", params=log_params))

            elif self.is_whitelist_last_item:
                self.whitelist["metadata"][key] = {
                    "status": "C",
                    "count": "1/1",
                    "size": parse_size_to_human_read(metadata["size"]),
                    "items": metadata["items"]
                }
                self.whitelist[key] = {value: info}

                logger.success(format_log_content(context="Whitelist complete", params=log_params))

            elif not self.is_whitelist_last_item:
                self.whitelist["metadata"][key] = {
                    "status": "I",
                    "count": f"1/{self.sub_whitelist_total_items}",
                    "size": parse_size_to_human_read(metadata["size"]),
                    "items": metadata["items"]
                }
                self.whitelist[key] = {value: info}

            self.whitelist["metadata"]["total_items"] = self.whitelist["metadata"]["total_items"] + metadata["items"]
            self.whitelist["metadata"]["total_size"] = parse_size_to_human_read(parse_size(self.whitelist["metadata"]["total_size"]) + metadata["size"])
            self.whitelist["metadata"]["updated"] = datetime.now(timezone.utc).isoformat()
        except Exception:
            logger.exception(format_log_content(context="Failed adding to whitelist", params=log_params))


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
                with open(self.whitelist_path, "w") as f:
                    json.dump(self.whitelist, f, indent=4)
                logger.success(f"Whitelist saved to: {self.whitelist_path}")
            except FileNotFoundError:
                logger.error(f"File not found: {self.whitelist_path}")
        else:
            logger.debug("No whitelist to be saved")


if __name__ == "__main__":
    # Initialize the Whitelist with a file path
    whitelist = Whitelist(whitelist_path="whitelist_test.json")

    # Add items to the whitelist
    whitelist.add_to_whitelist("FIPS:BR", "GHCND:BR000352000")
    whitelist.add_to_whitelist("FIPS:US", "GHCND:USW00094728")

    # Retrieve a specific whitelist entry
    br_whitelist = whitelist.retrieve_whitelist("FIPS:BR")
    print("BR Whitelist:", br_whitelist)

    # Retrieve the full whitelist
    full_whitelist = whitelist.retrieve_whitelist()
    print("Full Whitelist:", full_whitelist)

    # Save the whitelist to file
    whitelist.save_whitelist()