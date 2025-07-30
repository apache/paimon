from dataclasses import dataclass

from pypaimon.table.row.binary_row import BinaryRow


@dataclass
class SimpleStats:
    min_value: BinaryRow
    max_value: BinaryRow
    null_count: int


SIMPLE_STATS_SCHEMA = {
    "type": "record",
    "name": "SimpleStats",
    "namespace": "com.example.paimon",
    "fields": [
        {"name": "null_count", "type": ["null", "long"], "default": None},
        {"name": "min_value", "type": ["null", "bytes"], "default": None},
        {"name": "max_value", "type": ["null", "bytes"], "default": None},
    ]
}
