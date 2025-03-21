import os
#import sys
import json
import tempfile
import pytest
from lib.config_manager import load_config, get_config
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
#print("✅ test_main_utils.py loaded")
@pytest.fixture
def temp_config_file():
    content = {
        "databases": {
            "default": {
                "host": "localhost",
                "port": 5432
            }
        },
        "jobs": {
            "MyJob": {
                "db": "default",
                "param": "value"
            }
        }
    }
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as f:
        json.dump(content, f)
        f.flush()
        yield f.name
    os.remove(f.name)

def test_load_config(temp_config_file):
    config = load_config(temp_config_file)
    assert "jobs" in config

def test_get_config_valid(temp_config_file):
    config = load_config(temp_config_file)
    job_config = get_config(config, "MyJob")
    assert job_config["param"] == "value"
    assert job_config["db"]["host"] == "localhost"

def test_get_config_invalid_job(temp_config_file):
    config = load_config(temp_config_file)
    with pytest.raises(Exception, match="There is no UnknownJob job in the config file"):
        get_config(config, "UnknownJob")