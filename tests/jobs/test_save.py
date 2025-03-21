import pytest
from unittest.mock import patch, MagicMock
from jobs.save import Save
from pyspark.sql.types import StructType, StructField, StringType


@patch("jobs.save.eval")
def test_save_run_success(mock_eval):
    # Mock Spark session
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df_selected = MagicMock()
    mock_writer = MagicMock()
    mock_writer_options = MagicMock()
    mock_writer_mode = MagicMock()

    # Configure mocks
    mock_spark.read.parquet.return_value = mock_df
    mock_df.select.return_value = mock_df_selected
    mock_df_selected.write.format.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer_mode
    mock_writer_mode.save.return_value = None

    # Mock schema
    mock_schema = StructType([
        StructField("runtime_id", StringType(), True),
        StructField("city", StringType(), True),
    ])
    mock_eval.return_value = mock_schema

    config = {
        "input_path": "mock/path",
        "schema_name": "address_schema",  # This will be passed to eval()
        "fields": ["runtime_id", "city"],
        "db": {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "properties": {
                "user": "postgresuser",
                "password": "postgrespassword",
                "driver": "org.postgresql.Driver"
            }
        },
        "table_name": "tb_address"
    }

    # Run
    job = Save(mock_spark)
    job.run(config)

    # Assertions
    mock_spark.read.parquet.assert_called_once_with("mock/path", schema=mock_schema)
    mock_df.select.assert_called_once_with(["runtime_id", "city"])
    mock_writer.option.assert_any_call("url", config["db"]["url"])
    mock_writer.option.assert_any_call("dbtable", config["table_name"])
    mock_writer.option.assert_any_call("user", config["db"]["properties"]["user"])
    mock_writer.option.assert_any_call("password", config["db"]["properties"]["password"])
    mock_writer.option.assert_any_call("driver", config["db"]["properties"]["driver"])
    mock_writer.mode.assert_called_once_with("append")
    mock_writer_mode.save.assert_called_once()