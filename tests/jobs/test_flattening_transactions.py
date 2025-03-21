import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row, SparkSession
from jobs.flattening_transactions import FlatteningTransactions
from schemas.transaction import transaction_schema


@patch("jobs.flattening_transactions.to_timestamp")
@patch("jobs.flattening_transactions.concat_ws")
@patch("jobs.flattening_transactions.substring")
@patch("jobs.flattening_transactions.concat")
@patch("jobs.flattening_transactions.monotonically_increasing_id")
@patch("jobs.flattening_transactions.split")
@patch("jobs.flattening_transactions.explode")
@patch("jobs.flattening_transactions.from_json")
@patch("jobs.flattening_transactions.regexp_replace")
@patch("jobs.flattening_transactions.col")
@patch("jobs.flattening_transactions.optimize_partitioning")
@patch("jobs.flattening_transactions.compare_schemas")
def test_flattening_transactions_run_success(
    mock_compare_schemas, mock_optimize_partitioning,
    mock_col, mock_regexp_replace, mock_from_json, mock_explode,
    mock_split, mock_monotonically_increasing_id,
    mock_concat, mock_substring, mock_concat_ws, mock_to_timestamp
):
    mock_spark = MagicMock()
    mock_spark.sparkContext.defaultParallelism = 2

    # Mock DataFrame chain
    mock_df_raw = MagicMock(name="df_raw")
    mock_df_raw.rdd.getNumPartitions.return_value = 4
    mock_spark.read.parquet.return_value.select.return_value = mock_df_raw

    mock_df_optimized = MagicMock(name="df_optimized")
    mock_optimize_partitioning.return_value = mock_df_optimized

    # Job
    job = FlatteningTransactions(mock_spark)
    mock_df_fixed = MagicMock(name="df_fixed")
    mock_df_final = MagicMock(name="df_final")
    mock_df_final.schema = transaction_schema

    job._fix_json = MagicMock(return_value=mock_df_fixed)
    job._split_transaction_columns = MagicMock(return_value=mock_df_fixed)
    job._format_final_df = MagicMock(return_value=mock_df_final)

    # Handle chaining
    mock_df_fixed.withColumn.return_value = mock_df_fixed
    mock_df_fixed.drop.return_value = mock_df_fixed
    mock_df_final.write.mode.return_value.parquet.return_value = None

    config = {
        "input_path": "some/input/path",
        "output_path": "some/output/path"
    }

    # Run
    job.run(config)

    # ✅ Assertions
    mock_spark.read.parquet.assert_called_once_with("some/input/path", inferSchema=True)
    mock_optimize_partitioning.assert_called_once_with(mock_df_raw, 2, 4)
    job._fix_json.assert_called_once_with(mock_df_optimized)
    job._split_transaction_columns.assert_called_once()
    job._format_final_df.assert_called_once()
    mock_compare_schemas.assert_called_once()
    mock_df_final.write.mode.return_value.parquet.assert_called_once_with("some/output/path")


@patch("jobs.flattening_transactions.col")
@patch("jobs.flattening_transactions.regexp_replace")
@patch("jobs.flattening_transactions.lit")
def test_fix_json_chaining(mock_lit, mock_regexp_replace, mock_col):
    job = FlatteningTransactions(spark=MagicMock())

    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df  # Simulate chaining

    # Call the real method
    result = job._fix_json(mock_df)

    # ✅ Assert chaining still works
    assert result == mock_df
    assert mock_df.withColumn.call_count == 6


def test_split_transaction_columns_behavior():
    spark = SparkSession.builder.master("local[1]").appName("TestSplit").getOrCreate()
    job = FlatteningTransactions(spark)

    data = [
        ("id_1", '{"amount":"$100","transaction_date":"2022_10-2311:28:43.625377","tt":{"t":"transfer","from":"A","to":"B"}}'),
    ]
    df = spark.createDataFrame(data, ["runtime_id", "transaction"])
    result = job._split_transaction_columns(df, ",\"tt\":")

    assert "transaction" in result.columns
    assert "tt" in result.columns
    assert "runtime_id_transaction" in result.columns


def test_format_final_df_structure():
    spark = SparkSession.builder.master("local[1]").appName("TestFormat").getOrCreate()
    job = FlatteningTransactions(spark)

    # Create rows with Struct fields
    data = [
        Row(
            runtime_id="id_1",
            tt=Row(t="transfer", **{"from": "A", "to": "B"}),
            transaction=Row(amount="$123", transaction_date="2022_10-2311:28:43.625377")
        )
    ]
    df = spark.createDataFrame(data)

    result = job._format_final_df(df, "transaction", "tt")

    expected_columns = {"runtime_id", "client_from", "client_to", "amount", "transaction_date", "transaction_type"}
    assert set(result.columns) == expected_columns