import os

from loguru import logger
from typing import Optional


class Blacklist:
    def __init__(self, blacklist_path: Optional[str]=None):
        self.blacklist_path = blacklist_path
        self.blacklist = self._load_blacklist()

    def _load_blacklist(self) -> set:
        """Load the blacklist from a file."""
        blacklist = set()
        if self.blacklist_path and os.path.exists(self.blacklist_path):
            try:
                with open(self.blacklist_path, 'r') as f:
                    for line in f:
                        item = line.strip()
                        if item:  # Ignore empty lines
                            blacklist.add(item)
                logger.info(f"Blacklist loaded | Blacklisted items: {len(blacklist)}.")
            except Exception as e:
                logger.error(f"Error loading blacklist file: {e}")
                return set()

        return blacklist

    def add_to_blacklist(self, item):
        """Add an item to the blacklist."""
        self.blacklist.add(item)
        logger.debug("Blacklisting...")

    def remove_from_blacklist(self, item):
        """Remove an item from the blacklist."""
        self.blacklist.discard(item)

    def is_blacklisted(self, item):
        """Check if an item is blacklisted."""
        return item in self.blacklist
    
    def save_blacklist(self):
        """Save the blacklist to a file."""
        if self.blacklist_path is None:
            logger.error("Blacklist path is not set. Cannot save blacklist.")
            return
        with open(self.blacklist_path, 'w') as f:
            for item in self.blacklist:
                f.write(f"{item}\n")