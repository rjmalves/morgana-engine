def partitions_in_file(filename: str) -> dict[str, str]:
    parts = [p for p in filename.split("-")[1:] if len(p) > 0]
    partition_values: dict[str, str] = {}
    for p in parts:
        part_key_value = p.split("=")
        partition_values[part_key_value[0]] = part_key_value[1]
    return partition_values


def partition_value_in_file(filename: str, column: str) -> str | None:
    return partitions_in_file(filename).get(column)
