################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Test helper: subprocess ``ManifestListManager.read``; prints entry count on stdout.

Usage: ``python manifest_list_zstd_read_subprocess.py <warehouse> <table_id> <list_file_name>``
(``list_file_name`` = basename under the table manifest dir). Imports live in ``main()``."""

import sys


def main():
    if len(sys.argv) != 4:
        print(
            'usage: manifest_list_zstd_read_subprocess.py '
            '<warehouse_path> <table_id> <manifest_list_name>',
            file=sys.stderr,
        )
        return 2
    warehouse_path, catalog_table_id, manifest_list_name = (
        sys.argv[1],
        sys.argv[2],
        sys.argv[3],
    )
    from pypaimon.catalog.filesystem_catalog import FileSystemCatalog
    from pypaimon.common.identifier import Identifier
    from pypaimon.common.options import Options
    from pypaimon.common.options.config import CatalogOptions
    from pypaimon.manifest.manifest_list_manager import ManifestListManager

    catalog = FileSystemCatalog(Options({CatalogOptions.WAREHOUSE.key(): warehouse_path}))
    table = catalog.get_table(Identifier.from_string(catalog_table_id))
    manifest_list_manager = ManifestListManager(table)
    metas = manifest_list_manager.read(manifest_list_name)
    print(len(metas))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception:
        import traceback
        traceback.print_exc()
        raise SystemExit(1)
