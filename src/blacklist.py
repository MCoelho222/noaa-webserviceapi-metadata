import os

from loguru import logger
from typing import Optional


class Blacklist:
    def __init__(self, blacklist_path: Optional[str]=None):
        self.blacklist_path = blacklist_path
        self.blacklist = self.load_blacklist() if self.blacklist_path else set()

    def load_blacklist(self):
        """Load the blacklist from a file."""
        blacklist = set()
        if os.path.exists(self.blacklist_path):
            with open(self.blacklist_path, 'r') as f:
                for line in f:
                    item = line.strip()
                    if item:  # Ignore empty lines
                        blacklist.add(item)
        
        logger.info(f"Blacklist loaded | Blacklisted items: {len(blacklist)}.")
        return blacklist

    def add_to_blacklist(self, item):
        """Add an item to the blacklist."""
        self.blacklist.add(item)
        logger.info("Blacklisted")

    def remove_from_blacklist(self, item):
        """Remove an item from the blacklist."""
        self.blacklist.discard(item)

    def is_blacklisted(self, item):
        """Check if an item is blacklisted."""
        return item in self.blacklist
    
    def save_blacklist(self):
        """Save the blacklist to a file."""
        with open(self.blacklist_path, 'w') as f:
            for item in self.blacklist:
                f.write(f"{item}\n")