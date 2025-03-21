import pytest
from unittest.mock import MagicMock
from lib.utils import optimize_partitioning, diff_schemas, compare_schemas, normalize_schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


@pytest.mark.parametrize("num_cores, num_partitions,expected", [
    (4, 2, 8),     # Less than 2 * cores → scale up to 8
    (4, 20, 16),   # More than 4 * cores → scale down to 16
    (4, 10, None), # In range → no change
])
def test_optimize_partitioning(num_cores, num_partitions, expected):
    df = MagicMock(name="MockDataFrame")
    repartitioned_df = MagicMock(name="RepartitionedDataFrame")

    # Mock repartition behavior
    df.repartition.return_value = repartitioned_df

    result = optimize_partitioning(df, num_cores, num_partitions)

    if expected:
        df.repartition.assert_called_once_with(expected)
        assert result == repartitioned_df
    else:
        df.repartition.assert_not_called()
        assert result == df

def test_diff_schemas_detects_difference():
    schema1 = StructType([
        StructField("a", StringType()),
        StructField("b", IntegerType())
    ])
    schema2 = StructType([
        StructField("a", StringType()),
        StructField("b", StringType())
    ])
    diff = diff_schemas(schema1, schema2)
    assert "expected=string, actual=int" in diff or "expected=int, actual=string" in diff


def test_normalize_schema_sets_nullable_and_removes_metadata():
    original = StructType([
        StructField("a", StringType(), nullable=False, metadata={"meta": "value"}),
        StructField("b", IntegerType(), nullable=False)
    ])
    normalized = normalize_schema(original)
    for field in normalized.fields:
        assert field.nullable is True
        assert field.metadata == {}


def test_compare_schemas_passes_on_equal_structures():
    schema1 = StructType([
        StructField("a", StringType()),
        StructField("b", IntegerType())
    ])
    schema2 = StructType([
        StructField("b", IntegerType()),
        StructField("a", StringType())
    ])
    # Should not raise any error because we're comparing as sets
    compare_schemas(schema1, schema2)


def test_compare_schemas_raises_on_mismatch():
    schema1 = StructType([StructField("a", StringType())])
    schema2 = StructType([StructField("a", IntegerType())])
    with pytest.raises(Exception, match="Schema mismatch detected"):
        compare_schemas(schema1, schema2)