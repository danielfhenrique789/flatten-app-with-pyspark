#import os
#import sys
import pytest
from lib.main_utils import get_job_name
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
#print("✅ test_main_utils.py loaded")
def test_get_job_name_valid():
    args = ["main.py", "LoadRawData"]
    job = get_job_name(args)
    assert job == "LoadRawData"


def test_get_job_name_missing():
    with pytest.raises(ValueError) as exc_info:
        get_job_name(["main.py"])

    # Assert on the message
    assert "No job specified" in str(exc_info.value)