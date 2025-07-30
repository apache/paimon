from dataclasses import dataclass

from pypaimon.manifest.schema.simple_stats import SimpleStats


@dataclass
class ManifestFileMeta:
    file_name: str
    file_size: int
    num_added_files: int
    num_deleted_files: int
    partition_stats: SimpleStats
    schema_id: int
    min_bucket: int
    max_bucket: int
    min_level: int
    max_level: int


MANIFEST_FILE_META_SCHEMA = {
    "type": "record",
    "name": "ManifestFileMeta",
    "fields": [
        {"name": "_FILE_NAME", "type": "string"},
        {"name": "_FILE_SIZE", "type": "long"},
        {"name": "_NUM_ADDED_FILES", "type": "long"},
        {"name": "_NUM_DELETED_FILES", "type": "long"},
        {"name": "_PARTITION_STATS", "type": "long"},  # TODO
        {"name": "_SCHEMA_ID", "type": "long"},
    ]
}
