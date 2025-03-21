import os
import pytest
from unittest.mock import patch, MagicMock
from lib.auto_load import (
    convert_class_to_module,
    resolve_job_file,
    get_job_class,
)


# -----------------------
# Test: convert_class_to_module
# -----------------------
def test_convert_class_to_module():
    assert convert_class_to_module("MyAwesomeJob") == "my_awesome_job"
    assert convert_class_to_module("ReadTransactions") == "read_transactions"
    assert convert_class_to_module("ABC") == "a_b_c"
    assert convert_class_to_module("Job") == "job"


# -----------------------
# Test: resolve_job_file
# -----------------------
@patch("lib.auto_load.os.path.isfile")
def test_resolve_job_file_exact_match(mock_isfile):
    mock_isfile.side_effect = lambda path: "read_transactions.py" in path
    assert resolve_job_file("ReadTransactions", jobs_dir="src/jobs") == "ReadTransactions"


@patch("lib.auto_load.os.path.isfile")
def test_resolve_job_file_partial_match(mock_isfile):
    # Simulate full file doesn't exist but partial one does
    def mock_path(path):
        return "read.py" in path

    mock_isfile.side_effect = mock_path
    assert resolve_job_file("ReadSomethingElse", jobs_dir="src/jobs") == "Read"


@patch("lib.auto_load.os.path.isfile")
def test_resolve_job_file_not_found(mock_isfile):
    mock_isfile.return_value = False
    with pytest.raises(Exception, match="No matching job file found for 'WhateverJob'"):
        resolve_job_file("WhateverJob", jobs_dir="src/jobs")


# -----------------------
# Test: get_job_class
# -----------------------
@patch("lib.auto_load.resolve_job_file")
@patch("lib.auto_load.importlib.import_module")
def test_get_job_class_success(mock_import_module, mock_resolve):
    mock_resolve.return_value = "MyJob"
    mock_module = MagicMock()

    from base.spark_job import SparkJob

    # Dynamically create a mock SparkJob subclass
    DummyJob = type("MyJob", (SparkJob,), {})

    mock_module.MyJob = DummyJob
    mock_import_module.return_value = mock_module

    result = get_job_class("MyJob")
    assert result == DummyJob


@patch("lib.auto_load.resolve_job_file")
@patch("lib.auto_load.importlib.import_module")
def test_get_job_class_not_subclass(mock_import_module, mock_resolve):
    mock_resolve.return_value = "InvalidJob"
    mock_module = MagicMock()
    mock_module.InvalidJob = object  # Not subclass of SparkJob
    mock_import_module.return_value = mock_module

    with pytest.raises(ValueError, match="is not a valid SparkJob class"):
        get_job_class("InvalidJob")


@patch("lib.auto_load.resolve_job_file")
@patch("lib.auto_load.importlib.import_module")
def test_get_job_class_missing_class(mock_import_module, mock_resolve):
    mock_resolve.return_value = "MissingJob"
    mock_module = MagicMock()
    del mock_module.MissingJob
    mock_import_module.return_value = mock_module

    with pytest.raises(ValueError, match="not found"):
        get_job_class("MissingJob")