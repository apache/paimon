################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and
#  limitations under the License.
################################################################################

"""
Example script for cleaning orphan files from Paimon tables.
"""

import time
from pypaimon import CatalogFactory
from pypaimon.orphan_files_clean import execute_database_orphan_files


def clean_database(
    warehouse_path: str,
    database_name: str,
    older_than_hours: int = 24,
    dry_run: bool = False
):
    """Clean orphan files from a database.

    Args:
        warehouse_path: Path to the Paimon warehouse
        database_name: Name of the database
        older_than_hours: Delete files older than this many hours (default: 24 hours)
        dry_run: If True, only report what would be deleted
    """
    # Create catalog
    catalog = CatalogFactory.create({'warehouse': warehouse_path})

    # Calculate older_than_millis
    older_than_millis = int(time.time() * 1000) - (older_than_hours * 60 * 60 * 1000)

    # Clean orphan files for a specific table
    result = execute_database_orphan_files(
        catalog=catalog,
        database_name=database_name,
        table_name=None,  # Clean all tables
        older_than_millis=older_than_millis,
        dry_run=dry_run
    )

    # Print results
    print("=" * 60)
    print("Orphan Files Cleaning Results")
    print("=" * 60)
    print(f"Database: {database_name}")
    print(f"Deleted files: {result.deleted_file_count}")
    print(f"Total size: {result.deleted_file_total_len_in_bytes / (1024 * 1024):.2f} MB")
    print("=" * 60)

    if dry_run:
        print("[DRY RUN] No files were actually deleted.")
    else:
        print(f"[COMPLETED] Successfully cleaned {result.deleted_file_count} orphan files.")

    if result.deleted_files_path:
        print("\nDeleted files:")
        for file_path in result.deleted_files_path[:10]:  # Show first 10
            print(f"  - {file_path}")
        if len(result.deleted_files_path) > 10:
            print(f"  ... and {len(result.deleted_files_path) - 10} more files")


if __name__ == "__main__":
    # Example usage
    import sys

    if len(sys.argv) < 3:
        print("Usage: python orphan_files_clean_example.py <warehouse_path> <database_name>")
        print("Example: python orphan_files_clean_example.py /path/to/warehouse my_database")
        sys.exit(1)

    warehouse_path = sys.argv[1]
    database_name = sys.argv[2]

    # Optional: set dry_run=True to see what would be deleted without actually deleting
    clean_database(
        warehouse_path=warehouse_path,
        database_name=database_name,
        older_than_hours=24,  # Delete files older than 24 hours
        dry_run=False
    )
