import pytest
from unittest.mock import MagicMock, patch
from jobs.flattening_address import FlatteningAddress
from schemas.address import address_schema


@patch("jobs.flattening_address.from_json", return_value=MagicMock())
@patch("jobs.flattening_address.col", return_value=MagicMock())
@patch("jobs.flattening_address.optimize_partitioning")
@patch("jobs.flattening_address.compare_schemas")
def test_flattening_address_run_success(
    mock_compare_schemas,
    mock_optimize_partitioning,
    mock_col,
    mock_from_json
):
    mock_spark = MagicMock()
    mock_spark.sparkContext.defaultParallelism = 2

    mock_df_raw = MagicMock(name="df_raw")
    mock_df_raw.rdd.getNumPartitions.return_value = 4
    mock_spark.read.parquet.return_value.select.return_value = mock_df_raw

    mock_df_optimized = MagicMock(name="df_optimized")
    mock_optimize_partitioning.return_value = mock_df_optimized

    job = FlatteningAddress(mock_spark)
    mock_df_fixed = MagicMock(name="df_fixed")
    mock_df_final = MagicMock(name="df_final")
    mock_df_final.schema = address_schema

    job._fix_json = MagicMock(return_value=mock_df_fixed)
    job._format_final_df = MagicMock(return_value=mock_df_final)
    mock_df_final.write.mode.return_value.parquet.return_value = None

    config = {
        "input_path": "some/input/path",
        "output_path": "some/output/path"
    }

    job.run(config)

    mock_spark.read.parquet.assert_called_once_with("some/input/path", inferSchema=True)
    mock_optimize_partitioning.assert_called_once_with(mock_df_raw, 2, 4)
    job._fix_json.assert_called_once_with(mock_df_optimized)
    job._format_final_df.assert_called_once()
    mock_compare_schemas.assert_called_once_with(mock_df_final.schema, address_schema)
    mock_df_final.write.mode.return_value.parquet.assert_called_once_with("some/output/path")


@patch("jobs.flattening_address.col", return_value=MagicMock())
def test_format_final_df_structure(mock_col):
    job = FlatteningAddress(spark=MagicMock())
    mock_df = MagicMock()
    mock_selected_df = MagicMock()
    mock_df.select.return_value = mock_selected_df

    result = job._format_final_df(mock_df)

    assert result == mock_selected_df
    mock_df.select.assert_called_once()


from unittest.mock import patch, MagicMock
from jobs.flattening_address import FlatteningAddress

@patch("jobs.flattening_address.regexp_replace", return_value=MagicMock(name="regexp_replace"))
@patch("jobs.flattening_address.when", side_effect=lambda cond, val: MagicMock(name="when_expr"))
@patch("jobs.flattening_address.col", side_effect=lambda x: MagicMock(name=f"col({x})"))
def test_fix_json_chaining(mock_col, mock_when, mock_regexp_replace):
    job = FlatteningAddress(spark=MagicMock())
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df  # Enable chaining

    result = job._fix_json(mock_df)

    assert result == mock_df
    assert mock_df.withColumn.call_count == 7