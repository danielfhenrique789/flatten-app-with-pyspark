from pyspark.sql.types import StructType, StructField


def optimize_partitioning(df, num_cores, num_partitions):
    if num_partitions < num_cores * 2:
        df = df.repartition(num_cores * 2)
    elif num_partitions > num_cores * 4:
        df = df.repartition(num_cores * 4)
    return df


def diff_schemas(actual: StructType, expected: StructType) -> str:
    fields_actual = {f.name: f.dataType.simpleString() for f in actual.fields}
    fields_expected = {f.name: f.dataType.simpleString() for f in expected.fields}

    all_keys = sorted(set(fields_actual) | set(fields_expected))

    diffs = []
    for key in all_keys:
        a = fields_actual.get(key, "MISSING")
        e = fields_expected.get(key, "MISSING")
        if a != e:
            diffs.append(f"  • {key}: expected={e}, actual={a}")

    return "Schema differences:\n" + "\n".join(diffs)


def normalize_schema(schema: StructType):
    return StructType([
        StructField(f.name, f.dataType, True)  # Force nullable and drop metadata
        for f in schema.fields
    ])


def compare_schemas(actual: StructType, expected: StructType):
    def simplify(schema):
        return {(f.name, f.dataType.simpleString()) for f in schema.fields}

    actual_simple = simplify(actual)
    expected_simple = simplify(expected)

    if actual_simple != expected_simple:
        # Prepare diff message
        actual_dict = dict(actual_simple)
        expected_dict = dict(expected_simple)
        all_keys = sorted(set(actual_dict) | set(expected_dict))

        diff_lines = []
        for key in all_keys:
            actual_type = actual_dict.get(key)
            expected_type = expected_dict.get(key)

            if actual_type != expected_type:
                diff_lines.append(f"{key}: expected={expected_type}, actual={actual_type}")

        diff_msg = "\n".join(diff_lines) or "No differences found, but schemas differ (e.g., ordering or duplicates)"

        raise Exception(f"Schema mismatch detected:\n{diff_msg}")
