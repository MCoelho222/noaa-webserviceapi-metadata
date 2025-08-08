import json
import os
import pytest
import tempfile

from src.whitelist import Whitelist
from datetime import datetime, timezone


@pytest.fixture
def temp_wl_path():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
        yield tmp.name
    os.remove(tmp.name)


@pytest.fixture
def default_metadata():
    return {
        "items": 1,
        "size": 1024
    }


def test_initialize_whitelist():
    # Initialize the whitelist with a file path
    wl = Whitelist(
        wl_path="whitelist_test.json",
        wl_target="locationcategoryid",
        wl_description="testing"
    )
    
    assert all(item in wl.whitelist.keys() for item in ["target", "metadata", "description"])
    assert all(item in wl.whitelist["metadata"].keys() for item in ["created", "updated", "total_items", "total_size"])
    assert wl.whitelist["target"] == "locationcategoryid"
    assert wl.whitelist["description"] == "testing"

    # Check if the metadata created date (considering only YYYY-MM-DD) is today
    assert wl.whitelist["metadata"]["created"][:16] == datetime.now(timezone.utc).isoformat()[:16]
    assert wl.whitelist["metadata"]["updated"][:16] == datetime.now(timezone.utc).isoformat()[:16]

    # Check that total_items is correctly initiated as 0
    # and total size is correctly initiated as 0 B
    assert wl.whitelist["metadata"]["total_items"] == 0
    assert wl.whitelist["metadata"]["total_size"] == "0 B"


def test_initialize_whitelist_already_exists(temp_wl_path):
    # Create a temporary whitelist file
    with open(temp_wl_path, "w") as f:
        wl = {
            "target": "locationcategoryid",
            "description": "testing",
            "metadata": {
                "created": "2023-10-01T00:00:00+00:00",
                "updated": "2023-10-01T00:00:00+00:00",
                "total_items": 0,
                "total_size": "0 B"
            }
        }
        f.write(json.dumps(wl))

    # Check if the file exists
    assert os.path.exists(temp_wl_path)
    # Check if the file is not empty
    assert os.path.getsize(temp_wl_path) > 0
    # Check if the file is a valid JSON
    with open(temp_wl_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, dict)
        assert "target" in data
        assert "description" in data
        assert "metadata" in data
        assert "created" in data["metadata"]
        assert "updated" in data["metadata"]
        assert "total_items" in data["metadata"]
        assert "total_size" in data["metadata"]
    
    # Initialize the whitelist with the existing file path
    wl = Whitelist(
        wl_path=temp_wl_path,
        wl_target="locationcategoryid",
        wl_description="testing"
    )

    assert wl.whitelist["target"] == "locationcategoryid"
    assert wl.whitelist["description"] == "testing"
    assert wl.whitelist["metadata"]["created"] == "2023-10-01T00:00:00+00:00"
    assert wl.whitelist["metadata"]["updated"] == "2023-10-01T00:00:00+00:00"
    assert wl.whitelist["metadata"]["total_items"] == 0
    assert wl.whitelist["metadata"]["total_size"] == "0 B"



# def test_add_one_item_to_whitelist():
#     # Initialize the whitelist with a file path
#     wl = Whitelist(
#         wl_path="whitelist_test.json",
#         wl_target="locationcategoryid",
#         wl_description="testing"
#     )
    
#     metadata = {
#         "items": 1,
#         "size": 1000,
#     }
#     wl.sub_whitelist_total_items = 2
#     wl.add_to_whitelist("FIPS:BR", "GHCND:BR000352000", metadata)
#     # print(wl.whitelist)

#     assert wl.whitelist["metadata"]["FIPS:BR"]["status"] == "Incomplete"


# import os
# import pytest
# from unittest.mock import patch

# from src.whitelist import Whitelist





# @patch("utils.data.parse_size_to_human_read", return_value="1.0 KiB")
# @patch("utils.data.parse_size", return_value=0)
# def test_create_new_whitelist(mock_parse_size, mock_parse_human, temp_wl_path):
#     wl = Whitelist(wl_path=temp_wl_path, wl_target="locationid", wl_description="Test Desc")
#     assert "metadata" in wl.whitelist
#     assert wl.whitelist["target"] == "locationid"
#     assert wl.whitelist["description"] == "Test Desc"


# @patch("utils.data.parse_size_to_human_read", return_value="1.0 KiB")
# @patch("utils.data.parse_size", return_value=0)
# def test_add_to_whitelist_creates_new_key(mock_parse_size, mock_parse_human, temp_wl_path, default_metadata):
#     wl = Whitelist(wl_path=temp_wl_path)
#     wl.sub_whitelist_total_items = 10
#     wl.add_to_whitelist("FIPS:BR", "GHCND:BR0001", default_metadata)

#     assert "FIPS:BR" in wl.whitelist
#     assert "GHCND:BR0001" in wl.whitelist["FIPS:BR"]
#     assert wl.whitelist["metadata"]["FIPS:BR"]["status"] == "Incomplete"


# @patch("utils.data.parse_size_to_human_read", return_value="1.0 KiB")
# @patch("utils.data.parse_size", side_effect=[0, 1024])
# def test_add_to_whitelist_updates_existing_key(mock_parse_size, mock_parse_human, temp_wl_path, default_metadata):
#     wl = Whitelist(wl_path=temp_wl_path)
#     wl.sub_whitelist_total_items = 10
#     wl.add_to_whitelist("FIPS:BR", "GHCND:BR0001", default_metadata)
#     wl.add_to_whitelist("FIPS:BR", "GHCND:BR0002", default_metadata)

#     assert len(wl.whitelist["FIPS:BR"]) == 2
#     assert "GHCND:BR0002" in wl.whitelist["FIPS:BR"]
#     assert "count" in wl.whitelist["metadata"]["FIPS:BR"]


# def test_retrieve_whitelist_key_not_found(temp_wl_path):
#     wl = Whitelist(wl_path=temp_wl_path)
#     assert wl.retrieve_whitelist("NO_KEY") == {}


# def test_reset_whitelist(temp_wl_path):
#     wl = Whitelist(wl_path=temp_wl_path)
#     wl.sub_whitelist_total_items = 5
#     wl.is_sub_whitelist_complete = True
#     wl.reset_whitelist()
#     assert wl.sub_whitelist_total_items == 0
#     assert wl.is_sub_whitelist_complete is False


# @patch("utils.data.parse_size_to_human_read", return_value="1.0 KiB")
# @patch("utils.data.parse_size", return_value=0)
# def test_save_and_load_whitelist(mock_parse_size, mock_parse_human, temp_wl_path, default_metadata):
#     wl = Whitelist(wl_path=temp_wl_path)
#     wl.sub_whitelist_total_items = 10
#     wl.add_to_whitelist("FIPS:BR", "GHCND:BR0001", default_metadata)
#     wl.save_whitelist()

#     wl2 = Whitelist(wl_path=temp_wl_path)
#     assert "FIPS:BR" in wl2.whitelist


# @patch("utils.data.parse_size_to_human_read", return_value="1.0 KiB")
# def test_update_whitelist_status(mock_parse_human, temp_wl_path, default_metadata):
#     wl = Whitelist(wl_path=temp_wl_path)
#     wl.sub_whitelist_total_items = 10
#     wl.add_to_whitelist("FIPS:BR", "GHCND:BR0001", default_metadata)
#     wl.update_whitelist("FIPS:BR", "Complete")
#     assert wl.whitelist["metadata"]["FIPS:BR"]["status"] == "Complete"
