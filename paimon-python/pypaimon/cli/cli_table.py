# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Table commands for Paimon CLI.

This module provides table-related commands for the CLI.
"""

import argparse
import json
import sys
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from pypaimon.common.json_util import JSON


def cmd_table_read(args):
    """
    Execute the 'table read' command.
    
    Reads data from a Paimon table and displays it.
    
    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    
    # Load catalog configuration
    config_path = args.config
    config = load_catalog_config(config_path)
    
    # Create catalog
    catalog = create_catalog(config)
    
    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)
    
    database_name, table_name = parts
    
    # Get table
    try:
        table = catalog.get_table(f"{database_name}.{table_name}")
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)
    
    # Build read pipeline
    read_builder = table.new_read_builder()
    
    available_fields = set(field.name for field in table.table_schema.fields)

    # Parse select and where options
    select_columns = args.select
    where_clause = args.where
    user_columns = None
    extra_where_columns = []

    if select_columns:
        # Parse column names (comma-separated)
        user_columns = [col.strip() for col in select_columns.split(',')]

        # Validate that all columns exist in the table schema
        invalid_columns = [col for col in user_columns if col not in available_fields]
        if invalid_columns:
            print(f"Error: Column(s) {invalid_columns} do not exist in table '{table_identifier}'.", file=sys.stderr)
            sys.exit(1)

    # When both select and where are specified, ensure where-referenced fields
    # are included in the projection so the filter can work correctly.
    if user_columns and where_clause:
        from pypaimon.common.where_parser import extract_fields_from_where
        where_fields = extract_fields_from_where(where_clause, available_fields)
        user_column_set = set(user_columns)
        extra_where_columns = [f for f in where_fields if f not in user_column_set]
        projection_columns = user_columns + extra_where_columns
        read_builder = read_builder.with_projection(projection_columns)
    elif user_columns:
        read_builder = read_builder.with_projection(user_columns)

    # Apply where filter if specified
    if where_clause:
        from pypaimon.common.where_parser import parse_where_clause
        try:
            predicate = parse_where_clause(where_clause, table.table_schema.fields)
            if predicate:
                read_builder = read_builder.with_filter(predicate)
        except ValueError as e:
            print(f"Error: Invalid WHERE clause: {e}", file=sys.stderr)
            sys.exit(1)

    # Apply limit: only push down when there is no where clause,
    # because limit push-down may stop reading before enough rows
    # pass the filter, leading to fewer results than expected.
    limit = args.limit
    if limit and not where_clause:
        read_builder = read_builder.with_limit(limit)
    
    # Scan and read
    scan = read_builder.new_scan()
    plan = scan.plan()
    splits = plan.splits()
    
    read = read_builder.new_read()

    # Read splits incrementally, stopping early when limit is reached
    if limit:
        import pandas as pd
        collected_rows = 0
        table_list = []
        for split in splits:
            if collected_rows >= limit:
                break
            partial_df = read.to_pandas([split])
            collected_rows += len(partial_df)
            table_list.append(partial_df)
        df = pd.concat(table_list, ignore_index=True) if table_list else read.to_pandas([])
        if len(df) > limit:
            df = df.head(limit)
    else:
        df = read.to_pandas(splits)

    # Drop extra columns that were added only for where-clause filtering
    if extra_where_columns:
        df = df.drop(columns=extra_where_columns, errors='ignore')

    output_format = getattr(args, 'format', 'table')
    if output_format == 'json':
        import json
        print(json.dumps(df.to_dict(orient='records'), ensure_ascii=False))
    else:
        print(df.to_string(index=False))


def cmd_table_explain(args):
    """
    Execute the 'table explain' command.

    Prints the scan plan (snapshot, pushed-down predicate / projection /
    limit, partition / bucket / file-stats pruning funnel and split-
    level signals) without reading any data files.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    config = load_catalog_config(args.config)
    catalog = create_catalog(config)

    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)
    database_name, table_name = parts

    try:
        table = catalog.get_table(f"{database_name}.{table_name}")
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)

    read_builder = table.new_read_builder()
    available_fields = set(field.name for field in table.table_schema.fields)

    select_columns = getattr(args, 'select', None)
    if select_columns:
        user_columns = [col.strip() for col in select_columns.split(',')]
        invalid_columns = [col for col in user_columns if col not in available_fields]
        if invalid_columns:
            print(f"Error: Column(s) {invalid_columns} do not exist in table '{table_identifier}'.",
                  file=sys.stderr)
            sys.exit(1)
        read_builder = read_builder.with_projection(user_columns)

    where_clause = getattr(args, 'where', None)
    if where_clause:
        from pypaimon.common.where_parser import parse_where_clause
        try:
            predicate = parse_where_clause(where_clause, table.table_schema.fields)
            if predicate:
                read_builder = read_builder.with_filter(predicate)
        except ValueError as e:
            print(f"Error: Invalid WHERE clause: {e}", file=sys.stderr)
            sys.exit(1)

    # Unlike `table read`, explain always pushes the limit down — the
    # whole point of explain is to show what the planner will see,
    # including limit pushdown.
    limit = getattr(args, 'limit', None)
    if limit is not None:
        read_builder = read_builder.with_limit(limit)

    verbose = getattr(args, 'verbose', False)
    try:
        result = read_builder.explain(verbose=verbose)
    except Exception as e:
        print(f"Error: Failed to explain table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)

    output_format = getattr(args, 'format', 'table')
    if output_format == 'json':
        import json
        print(json.dumps(_explain_result_to_json_dict(result), indent=2, ensure_ascii=False))
    else:
        print(str(result))


def _explain_result_to_json_dict(result):
    """Serialize an ``ExplainResult`` to a JSON-friendly dict.

    ``level_histogram`` has ``int`` keys, both at the top level and
    inside each split. ``json.dumps`` would coerce them to strings
    silently; we do it up front so the output is explicit and stable.
    """
    payload = asdict(result)
    payload['level_histogram'] = {
        str(level): count for level, count in payload.get('level_histogram', {}).items()
    }
    if payload.get('splits') is not None:
        for split in payload['splits']:
            split['level_histogram'] = {
                str(level): count for level, count in split.get('level_histogram', {}).items()
            }
    return payload


def cmd_table_full_text_search(args):
    """
    Execute the 'table full-text-search' command.

    Performs full-text search on a Paimon table and displays matching rows.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    config_path = args.config
    config = load_catalog_config(config_path)
    catalog = create_catalog(config)

    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)

    database_name, table_name = parts

    try:
        table = catalog.get_table(f"{database_name}.{table_name}")
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)

    limit = args.limit

    try:
        builder = table.new_full_text_search_builder()
        builder.with_query(args.column, args.query)
        builder.with_limit(limit)
        result = builder.execute_local()
    except Exception as e:
        print(f"Error: Full-text search failed: {e}", file=sys.stderr)
        sys.exit(1)

    if result.is_empty():
        print("No matching rows found.")
        return

    # Read matching rows using global index result
    read_builder = table.new_read_builder()

    select_columns = args.select
    if select_columns:
        projection = [col.strip() for col in select_columns.split(',')]
        available_fields = set(field.name for field in table.table_schema.fields)
        invalid_columns = [col for col in projection if col not in available_fields]
        if invalid_columns:
            print(f"Error: Column(s) {invalid_columns} do not exist in table '{table_identifier}'.",
                  file=sys.stderr)
            sys.exit(1)
        read_builder = read_builder.with_projection(projection)

    scan = read_builder.new_scan().with_global_index_result(result)
    plan = scan.plan()
    splits = plan.splits()
    read = read_builder.new_read()
    df = read.to_pandas(splits)

    output_format = getattr(args, 'format', 'table')
    if output_format == 'json':
        import json
        print(json.dumps(df.to_dict(orient='records'), ensure_ascii=False))
    else:
        print(df.to_string(index=False))


def cmd_table_get(args):
    """
    Execute the 'table get' command.
    
    Gets and displays table schema information in JSON format.
    
    Args:
        args: Parsed command line arguments.
    """
    table = _open_schema_table(args)
    schema_id = getattr(args, 'schema_id', None)
    snapshot_id = getattr(args, 'snapshot_id', None)
    try:
        if snapshot_id is not None:
            snapshot = table.snapshot_manager().get_snapshot_by_id(snapshot_id)
            if snapshot is None:
                raise ValueError(f"Snapshot {snapshot_id} not found for table '{args.table}'.")
            schema_id = snapshot.schema_id
            if schema_id is None:
                raise ValueError(f"Snapshot {snapshot_id} does not contain a schema ID.")
        table_schema = table.table_schema if schema_id is None else table.schema_manager.get_schema(schema_id)
        if table_schema is None:
            raise ValueError(f"Schema {schema_id} not found for table '{args.table}'.")
        
        # Get table schema and convert to Schema, then output as JSON
        schema = table_schema.to_schema()
        if getattr(args, 'format', 'json') == 'json':
            print(JSON.to_json(schema, indent=2))
        else:
            import pandas as pd
            values = json.loads(JSON.to_json(schema))
            rows = [[key, _snapshot_display_value(value)] for key, value in values.items()]
            print(pd.DataFrame(rows, columns=['Field', 'Value']).to_string(index=False))
    except Exception as e:
        target = 'latest schema' if schema_id is None else f'schema {schema_id}'
        if snapshot_id is not None:
            target = f'schema for snapshot {snapshot_id}'
        print(f"Error: Failed to get {target} for table '{args.table}': {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_list_schemas(args):
    """
    Execute the 'table list-schemas' command.
    
    Lists available schemas in ascending schema-ID order without reading table data.
    
    Args:
        args: Parsed command line arguments.
    """
    table = _open_schema_table(args)
    try:
        manager = table.schema_manager
        schemas = []
        for schema_id in sorted(manager._list_versioned_files()):
            schema = manager.get_schema(schema_id)
            if schema is None:
                continue
            schemas.append(schema)
            if args.limit is not None and len(schemas) >= args.limit:
                break
        
        if args.format == 'json':
            print(json.dumps([json.loads(JSON.to_json(s)) for s in schemas], ensure_ascii=False, indent=2))
        elif not schemas:
            print(f"No schemas found for table '{args.table}'.")
        else:
            import pandas as pd
            rows = []
            for s in schemas:
                row = [str(s.id)]
                for value in ([f.to_dict() for f in s.fields], s.partition_keys, s.primary_keys, s.options):
                    row.append(json.dumps(value, ensure_ascii=False, separators=(',', ':')))
                update_time = _snapshot_commit_time(s.time_millis) if s.time_millis is not None else '-'
                row.extend([s.comment, update_time])
                rows.append(row)
            # Convert before Pandas can coerce nullable integers to floats.
            rows = [[_snapshot_display_value(value) for value in row] for row in rows]
            columns = ['schema_id', 'fields', 'partition_keys', 'primary_keys', 'options', 'comment', 'update_time']
            print(pd.DataFrame(rows, columns=columns).to_string(index=False))
    except Exception as e:
        print(f"Error: Failed to list schemas for table '{args.table}': {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_snapshot(args):
    """
    Execute the 'table snapshot' command.
    
    Gets and displays the latest snapshot of a Paimon table in JSON format.
    
    Args:
        args: Parsed command line arguments.
    """
    manager = _open_snapshot_manager(args)
    snapshot_id = getattr(args, 'snapshot_id', None)
    try:
        snapshot = (
            # Get latest snapshot
            manager.get_latest_snapshot() if snapshot_id is None
            else manager.get_snapshot_by_id(snapshot_id)
        )
        if snapshot is None:
            if snapshot_id is None:
                raise ValueError(f"No snapshot found for table '{args.table}'.")
            raise ValueError(f"Snapshot {snapshot_id} not found for table '{args.table}'.")
        
        if getattr(args, 'format', 'json') == 'json':
            # Output snapshot as JSON
            print(JSON.to_json(snapshot, indent=2))
        else:
            import pandas as pd
            values = json.loads(JSON.to_json(snapshot))
            rows = [[key, _snapshot_display_value(value)] for key, value in values.items()]
            for row in rows:
                if row[0] == 'timeMillis':
                    row[1] += ' ({})'.format(_snapshot_commit_time(snapshot.time_millis))
            print(pd.DataFrame(rows, columns=['Field', 'Value']).to_string(index=False))
    except Exception as e:
        target = 'latest snapshot' if snapshot_id is None else f'snapshot {snapshot_id}'
        print(f"Error: Failed to get {target} for table '{args.table}': {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_list_snapshots(args):
    """
    Execute the 'table list-snapshots' command.
    
    Lists retained snapshots in ascending snapshot-ID order without reading table data.
    
    Args:
        args: Parsed command line arguments.
    """
    manager = _open_snapshot_manager(args)
    try:
        snapshots = []
        for snapshot_id in manager.list_snapshot_ids():
            try:
                snapshot = manager.get_snapshot_by_id(snapshot_id)
            except FileNotFoundError:
                # Expiration can remove a file between listing and opening it.
                continue
            if snapshot is None:
                continue
            snapshots.append(snapshot)
            if args.limit is not None and len(snapshots) >= args.limit:
                break
        
        if args.format == 'json':
            # Preserve Snapshot's JSON field names, optional values and integers.
            print(json.dumps([json.loads(JSON.to_json(s)) for s in snapshots], ensure_ascii=False, indent=2))
        elif not snapshots:
            print(f"No snapshots found for table '{args.table}'.")
        else:
            import pandas as pd
            rows = [[s.id, s.schema_id, _snapshot_commit_time(s.time_millis),
                     s.commit_kind, s.commit_user, s.total_record_count,
                     s.delta_record_count, s.changelog_record_count] for s in snapshots]
            # Convert before Pandas can coerce nullable integers to floats.
            rows = [[_snapshot_display_value(value) for value in row] for row in rows]
            columns = ['Snapshot ID', 'Schema ID', 'Commit Time (UTC)', 'Commit Kind',
                       'Commit User', 'Total Records', 'Delta Records', 'Changelog Records']
            print(pd.DataFrame(rows, columns=columns).to_string(index=False))
    except Exception as e:
        print(f"Error: Failed to list snapshots for table '{args.table}': {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_list_manifests(args):
    """
    Execute the 'table list-manifests' command.
    
    Lists the base, delta and changelog manifests referenced by a snapshot.
    
    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.manifest.manifest_list_manager import ManifestListManager
    from pypaimon.utils.row_to_string import cast_row_to_string

    table = _open_manifest_table(args)
    try:
        manager = table.snapshot_manager()
        snapshot = (manager.get_latest_snapshot() if args.snapshot_id is None
                    else manager.get_snapshot_by_id(args.snapshot_id))
        if snapshot is None and args.snapshot_id is not None:
            raise ValueError(f"Snapshot {args.snapshot_id} not found for table '{args.table}'.")

        manifests = []
        kinds = ('base', 'delta', 'changelog') if args.type == 'all' else (args.type,)
        if snapshot is not None:
            list_manager = ManifestListManager(table)
            for kind in kinds:
                list_name = getattr(snapshot, kind + '_manifest_list')
                if not list_name:
                    continue
                list_size = getattr(snapshot, kind + '_manifest_list_size')
                for meta in list_manager.read(list_name, list_size):
                    manifests.append({
                        'type': kind,
                        'manifest_list': list_name,
                        'file_name': meta.file_name,
                        'file_size': meta.file_size,
                        'num_added_files': meta.num_added_files,
                        'num_deleted_files': meta.num_deleted_files,
                        'schema_id': meta.schema_id,
                        'min_partition_stats': cast_row_to_string(meta.partition_stats.min_values),
                        'max_partition_stats': cast_row_to_string(meta.partition_stats.max_values),
                        'min_row_id': meta.min_row_id,
                        'max_row_id': meta.max_row_id,
                    })
                    if args.limit is not None and len(manifests) >= args.limit:
                        break
                if args.limit is not None and len(manifests) >= args.limit:
                    break

        snapshot_id = snapshot.id if snapshot is not None else None
        if args.format == 'json':
            print(json.dumps({'table': args.table, 'snapshot_id': snapshot_id, 'manifests': manifests},
                             ensure_ascii=False, indent=2, allow_nan=False))
        else:
            print(f"Table: {args.table}\nSnapshot: {_snapshot_display_value(snapshot_id)}")
            if not manifests:
                print(f"No manifests found for table '{args.table}'.")
                return
            import pandas as pd
            columns = ['file_name', 'file_size', 'num_added_files', 'num_deleted_files', 'schema_id',
                       'min_partition_stats', 'max_partition_stats', 'min_row_id', 'max_row_id']
            for kind in kinds:
                group = [meta for meta in manifests if meta['type'] == kind]
                if not group:
                    continue
                print('\n{} ({}):'.format(kind, group[0]['manifest_list']))
                # Convert before Pandas can coerce nullable row IDs to floats.
                rows = [[_snapshot_display_value(meta[column]) for column in columns] for meta in group]
                print(pd.DataFrame(rows, columns=columns).to_string(index=False))
    except Exception as e:
        print(f"Error: Failed to list manifests for table '{args.table}': {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_manifest(args):
    """
    Execute the 'table manifest' command.
    
    Displays a manifest's original ADD/DELETE entries and complete metadata.
    
    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.manifest.manifest_file_manager import ManifestFileManager

    table = _open_manifest_table(args)
    try:
        name = args.file
        if not name or name in ('.', '..') or any(char in name for char in ('/', '\\', '\x00')):
            raise ValueError('--file must be a file name within the table manifest directory.')
        manager = ManifestFileManager(table)
        # Reading a single file preserves DELETE entries and their original order.
        entries = manager.read(name, drop_stats=False)
        result = {
            'table': args.table,
            'manifest_file': name,
            'manifest_path': manager.manifest_path + '/' + name,
            'entries': [_manifest_entry_to_dict(table, entry) for entry in entries],
        }
        result = _manifest_json_value(result)
        if getattr(args, 'format', 'json') == 'json':
            print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
        else:
            import pandas as pd
            rows = [[key, _snapshot_display_value(value)] for key, value in result.items()]
            print(pd.DataFrame(rows, columns=['Field', 'Value']).to_string(index=False))
    except Exception as e:
        print(f"Error: Failed to read manifest '{args.file}' for table '{args.table}': {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_list_files(args):
    """
    Execute the 'table list-files' command.

    Lists the live data files referenced by the latest or a specified snapshot.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.table.file_store_table import FileStoreTable
    from pypaimon.table.system.files_table import FilesTable

    table = _open_schema_table(args)
    try:
        if not isinstance(table, FileStoreTable):
            raise ValueError('File listing is not supported for this table type; expected a FileStoreTable.')
        manager = table.snapshot_manager()
        snapshot = (manager.get_latest_snapshot() if args.snapshot_id is None
                    else manager.get_snapshot_by_id(args.snapshot_id))
        if snapshot is None and args.snapshot_id is not None:
            raise ValueError(f"Snapshot {args.snapshot_id} not found for table '{args.table}'.")

        # Pin the resolved snapshot, including an empty table, before reading its live files.
        files = FilesTable(table._copy_with_snapshot(snapshot))._build_arrow_table()
        if args.bucket is not None:
            import pyarrow.compute as pc
            files = files.filter(pc.equal(files.column('bucket'), args.bucket))
        if args.limit is not None:
            files = files.slice(0, args.limit)
        files = files.rename_columns(['delete_row_count' if name == 'deleteRowCount' else name
                                      for name in files.column_names])
        snapshot_id = snapshot.id if snapshot is not None else None
        # Table.to_pylist is unavailable in the supported PyArrow 6 release.
        columns = files.to_pydict()
        rows = [dict(snapshot_id=snapshot_id, **dict(zip(columns, values))) for values in zip(*columns.values())]
        if args.format == 'json':
            result = _manifest_json_value({'table': args.table, 'snapshot_id': snapshot_id, 'files': rows})
            print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
        elif not rows:
            print(f"No files found for table '{args.table}' (snapshot {_snapshot_display_value(snapshot_id)}).")
        else:
            import pandas as pd
            for row in rows:
                if row['creation_time'] is not None:
                    row['creation_time'] = row['creation_time'].isoformat()
            # Convert before Pandas can coerce nullable integers to floats.
            values = [[_snapshot_display_value(value) for value in row.values()] for row in rows]
            print(pd.DataFrame(values, columns=['snapshot_id'] + files.column_names).to_string(index=False))
    except Exception as e:
        print(f"Error: Failed to list files for table '{args.table}': {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_file(args):
    """
    Execute the 'table file' command.

    Displays a Parquet file's schema and footer metadata without reading data records.

    Args:
        args: Parsed command line arguments.
    """
    from os.path import basename

    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    from pypaimon.table.system.files_table import _data_file_format

    table = _open_schema_table(args)
    try:
        status = table.file_io.get_file_status(args.file)
        if status.type != pafs.FileType.File:
            raise ValueError('--file must point to a regular file.')
        file_format = _data_file_format(basename(status.path)).lower()
        if file_format != 'parquet':
            raise ValueError('Unsupported file format. Only Parquet files are supported.')
        with table.file_io.new_input_stream(args.file) as stream:
            parquet_file = pq.ParquetFile(stream)
            metadata = parquet_file.metadata.to_dict()
            columns = []
            for i in range(parquet_file.metadata.num_columns):
                column = parquet_file.schema.column(i)
                columns.append({
                    'name': column.name,
                    'path': column.path,
                    'physical_type': column.physical_type,
                    'logical_type': str(column.logical_type),
                    'converted_type': column.converted_type,
                    'max_definition_level': column.max_definition_level,
                    'max_repetition_level': column.max_repetition_level,
                    'length': column.length,
                    'precision': column.precision,
                    'scale': column.scale,
                })
            result = _manifest_json_value({
                'table': args.table,
                'file_path': args.file,
                'file_format': file_format,
                'file_size_in_bytes': status.size,
                'schema': {'arrow': str(parquet_file.schema_arrow), 'parquet': columns},
                'metadata': metadata,
            })
        if args.format == 'json':
            print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
        else:
            field_width = max(len(key) for key in result)
            print(f'{"Field":<{field_width}}  Value')
            for key, value in result.items():
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)
                else:
                    value = _snapshot_display_value(value)
                for i, line in enumerate(value.splitlines() or ['']):
                    field = key if i == 0 else ''
                    print(f'{field:<{field_width}}  {line}')
    except Exception as e:
        print(f"Error: Failed to inspect file '{args.file}' for table '{args.table}': {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_rollback(args):
    """
    Execute the 'table rollback' command.

    Rolls a table back to an earlier snapshot, tag, or timestamp using the
    table's existing rollback API. Exactly one of --snapshot-id / --tag /
    --timestamp must be provided.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    from pypaimon.table.file_store_table import FileStoreTable

    config = load_catalog_config(args.config)
    catalog = create_catalog(config)

    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)

    database_name, table_name = parts
    try:
        table = catalog.get_table(f"{database_name}.{table_name}")
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(table, FileStoreTable):
        print(f"Error: Table '{table_identifier}' is not a FileStoreTable. "
              f"Rollback operation is not supported for this table type.", file=sys.stderr)
        sys.exit(1)

    try:
        if args.snapshot_id is not None:
            table.rollback_to(args.snapshot_id)
            target_desc = f"snapshot {args.snapshot_id}"
        elif args.tag is not None:
            table.rollback_to(args.tag)
            target_desc = f"tag '{args.tag}'"
        else:
            table.rollback_to_timestamp(args.timestamp)
            target_desc = f"timestamp {args.timestamp}"
    except Exception as e:
        print(f"Error: Failed to roll back table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Rolled back table '{table_identifier}' to {target_desc}.")


def cmd_table_create(args):
    """
    Execute the 'table create' command.
    
    Creates a new Paimon table with the specified schema.
    
    Args:
        args: Parsed command line arguments.
    """
    import json
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    from pypaimon import Schema
    
    # Load catalog configuration
    config_path = args.config
    config = load_catalog_config(config_path)
    
    # Create catalog
    catalog = create_catalog(config)
    
    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)
    
    database_name, table_name = parts
    
    # Load schema from JSON file
    schema_file = args.schema
    if not schema_file:
        print("Error: Schema is required. Use --schema option.", file=sys.stderr)
        sys.exit(1)
    
    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_json = f.read()
        paimon_schema = JSON.from_json(schema_json, Schema)
        
    except FileNotFoundError:
        print(f"Error: Schema file not found: {schema_file}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in schema file: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to parse schema: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Create table
    try:
        ignore_if_exists = args.ignore_if_exists
        catalog.create_table(f"{database_name}.{table_name}", paimon_schema, ignore_if_exists)
        
        print(f"Table '{database_name}.{table_name}' created successfully.")
        
    except Exception as e:
        print(f"Error: Failed to create table: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_import(args):
    """
    Execute the 'table import' command.
    
    Imports data from a CSV or JSON file into a Paimon table.
    
    Args:
        args: Parsed command line arguments.
    """
    import pandas as pd
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    
    # Load catalog configuration
    config_path = args.config
    config = load_catalog_config(config_path)
    
    # Create catalog
    catalog = create_catalog(config)
    
    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)
    
    database_name, table_name = parts
    
    # Get table
    try:
        table = catalog.get_table(f"{database_name}.{table_name}")
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)
    
    # Get input file path
    input_file = args.input
    if not input_file:
        print("Error: Input file is required. Use --input option.", file=sys.stderr)
        sys.exit(1)
    
    # Read data from file
    try:
        file_lower = input_file.lower()
        if file_lower.endswith('.csv'):
            # Read CSV file
            df = pd.read_csv(input_file)
        elif file_lower.endswith('.json'):
            # Read JSON file
            df = pd.read_json(input_file)
        else:
            print("Error: Unsupported file format. Only CSV and JSON files are supported.", file=sys.stderr)
            sys.exit(1)
        
        if df.empty:
            print("Warning: No data found in file '{input_file}'.", file=sys.stderr)
            return
        
    except FileNotFoundError:
        print(f"Error: Input file not found: {input_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to read input file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Write data to table
    table_write = None
    table_commit = None
    try:
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        
        # Get table schema and convert DataFrame to match it
        import pyarrow as pa
        from pypaimon.schema.data_types import PyarrowFieldParser
        pa_schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
        
        # Convert DataFrame to PyArrow Table with the correct schema
        table_data = pa.Table.from_pandas(df, schema=pa_schema)
        
        # Write data
        table_write.write_arrow(table_data)
        
        # Commit write
        table_commit.commit(table_write.prepare_commit())
        
        print(f"Successfully imported {len(df)} rows into '{database_name}.{table_name}'.")
        
    except Exception as e:
        print(f"Error: Failed to import data: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if table_write is not None:
            table_write.close()
        if table_commit is not None:
            table_commit.close()


def cmd_table_drop(args):
    """
    Execute the 'table drop' command.
    
    Drops a Paimon table.
    
    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    
    # Load catalog configuration
    config_path = args.config
    config = load_catalog_config(config_path)
    
    # Create catalog
    catalog = create_catalog(config)
    
    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)
    
    database_name, table_name = parts
    
    # Drop table
    try:
        ignore_if_not_exists = args.ignore_if_not_exists
        catalog.drop_table(f"{database_name}.{table_name}", ignore_if_not_exists)
        
        print(f"Table '{database_name}.{table_name}' dropped successfully.")
        
    except Exception as e:
        print(f"Error: Failed to drop table: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_rename(args):
    """
    Execute the 'table rename' command.

    Renames a Paimon table.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    config_path = args.config
    config = load_catalog_config(config_path)
    catalog = create_catalog(config)

    source_table = args.table
    target_table = args.target

    source_parts = source_table.split('.')
    if len(source_parts) != 2:
        print(f"Error: Invalid source table identifier '{source_table}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)

    target_parts = target_table.split('.')
    if len(target_parts) != 2:
        print(f"Error: Invalid target table identifier '{target_table}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)

    try:
        catalog.rename_table(source_table, target_table)
        print(f"Table '{source_table}' renamed to '{target_table}' successfully.")
    except NotImplementedError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to rename table: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_alter(args):
    """
    Execute the 'table alter' command.

    Alters a Paimon table with the specified schema changes.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    from pypaimon.schema.schema_change import SchemaChange
    from pypaimon.schema.data_types import DataTypeParser

    # Load catalog configuration
    config_path = args.config
    config = load_catalog_config(config_path)

    # Create catalog
    catalog = create_catalog(config)

    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)

    database_name, table_name = parts

    # Build schema change based on alter subcommand
    alter_command = args.alter_command
    changes = []

    if alter_command == 'set-option':
        changes.append(SchemaChange.set_option(args.key, args.value))
    elif alter_command == 'remove-option':
        changes.append(SchemaChange.remove_option(args.key))
    elif alter_command == 'add-column':
        from pypaimon.schema.schema_change import Move
        move = None
        if getattr(args, 'first', False):
            move = Move.first(args.name)
        elif getattr(args, 'after', None):
            move = Move.after(args.name, args.after)
        data_type = DataTypeParser.parse_atomic_type_sql_string(args.type)
        changes.append(SchemaChange.add_column(args.name, data_type, comment=args.comment, move=move))
    elif alter_command == 'drop-column':
        changes.append(SchemaChange.drop_column(args.name))
    elif alter_command == 'rename-column':
        changes.append(SchemaChange.rename_column(args.name, args.new_name))
    elif alter_command == 'update-comment':
        changes.append(SchemaChange.update_comment(args.comment))
    elif alter_command == 'alter-column':
        from pypaimon.schema.schema_change import Move
        column_name = args.name
        has_action = False
        if getattr(args, 'type', None):
            new_type = DataTypeParser.parse_atomic_type_sql_string(args.type)
            changes.append(SchemaChange.update_column_type(column_name, new_type))
            has_action = True
        if getattr(args, 'comment', None) is not None:
            changes.append(SchemaChange.update_column_comment(column_name, args.comment))
            has_action = True
        if getattr(args, 'first', False):
            changes.append(SchemaChange.update_column_position(Move.first(column_name)))
            has_action = True
        elif getattr(args, 'after', None):
            changes.append(SchemaChange.update_column_position(Move.after(column_name, args.after)))
            has_action = True
        if not has_action:
            print("Error: At least one of --type, --comment, --first, or --after must be specified.",
                  file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: Unknown alter command '{alter_command}'.", file=sys.stderr)
        sys.exit(1)

    # Apply schema changes
    try:
        ignore_if_not_exists = args.ignore_if_not_exists
        catalog.alter_table(f"{database_name}.{table_name}", changes, ignore_if_not_exists)
        print(f"Table '{database_name}.{table_name}' altered successfully.")
    except Exception as e:
        print(f"Error: Failed to alter table: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_list_partitions(args):
    """
    Execute the 'table list-partitions' command.

    Lists partitions of a Paimon table with optional pattern filtering.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    # Load catalog configuration
    config_path = args.config
    config = load_catalog_config(config_path)

    # Create catalog
    catalog = create_catalog(config)

    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)

    # List partitions with pagination
    pattern = getattr(args, 'pattern', None)
    try:
        paged_list = catalog.list_partitions_paged(
            table_identifier,
            partition_name_pattern=pattern,
        )
        import pandas as pd

        partitions = paged_list.elements
        if not partitions:
            print("No partitions found.")
            return

        data = []
        for p in partitions:
            spec_str = ",".join(f"{k}={v}" for k, v in p.spec.items())
            data.append({
                'Partition': spec_str,
                'RecordCount': p.record_count,
                'FileSizeInBytes': p.file_size_in_bytes,
                'FileCount': p.file_count,
                'LastFileCreationTime': p.last_file_creation_time,
                'UpdatedAt': p.updated_at,
                'UpdatedBy': p.updated_by or '',
            })

        output_format = getattr(args, 'format', 'table')
        if output_format == 'json':
            import json
            print(json.dumps(data, ensure_ascii=False))
        else:
            df = pd.DataFrame(data)
            print(df.to_string(index=False))

    except NotImplementedError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to list partitions: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_drop_partition(args):
    """
    Execute the 'table drop-partition' command.

    Drops one or more partitions from a Paimon table.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    # Load catalog configuration
    config_path = args.config
    config = load_catalog_config(config_path)

    # Create catalog
    catalog = create_catalog(config)

    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)

    # Parse partition specs
    partition_strings = args.partition
    if not partition_strings:
        print("Error: At least one --partition must be specified.", file=sys.stderr)
        sys.exit(1)

    partitions = []
    for partition_str in partition_strings:
        partition_dict = {}
        for kv in partition_str.split(','):
            kv = kv.strip()
            if '=' not in kv:
                print(f"Error: Invalid partition spec '{kv}'. "
                      f"Expected format: 'key=value'", file=sys.stderr)
                sys.exit(1)
            key, value = kv.split('=', 1)
            partition_dict[key.strip()] = value.strip()
        if not partition_dict:
            print("Error: Empty partition spec.", file=sys.stderr)
            sys.exit(1)
        partitions.append(partition_dict)

    # Drop partitions
    try:
        catalog.drop_partitions(table_identifier, partitions)
        partition_desc = "; ".join(
            ",".join(f"{k}={v}" for k, v in p.items()) for p in partitions
        )
        print(f"Successfully dropped partition(s) [{partition_desc}] "
              f"from table '{table_identifier}'.")
    except Exception as e:
        print(f"Error: Failed to drop partition(s): {e}", file=sys.stderr)
        sys.exit(1)


def _open_schema_table(args):
    """Load a file-store table for the schema inspection commands."""
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    
    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2 or not all(parts):
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)
    
    database_name, table_name = parts
    
    try:
        # Load catalog configuration
        config_path = args.config
        config = load_catalog_config(config_path)
        
        # Create catalog
        catalog = create_catalog(config)
        
        # Get table
        table = catalog.get_table(f"{database_name}.{table_name}")
        return table
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)


def _open_snapshot_manager(args):
    """Load a file-store table for the snapshot inspection commands."""
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    from pypaimon.table.file_store_table import FileStoreTable
    
    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2 or not all(parts):
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Load catalog configuration
        config = load_catalog_config(args.config)
        
        # Create catalog
        catalog = create_catalog(config)
        
        # Get table
        table = catalog.get_table(table_identifier)
        
        # Check if table is FileStoreTable
        if not isinstance(table, FileStoreTable):
            raise ValueError('Snapshot operation is not supported for this table type; '
                             'expected a FileStoreTable.')
        return table.snapshot_manager()
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)


def _open_manifest_table(args):
    """Load a file-store table for the manifest inspection commands."""
    from pypaimon.table.file_store_table import FileStoreTable

    table = _open_schema_table(args)
    if not isinstance(table, FileStoreTable):
        print('Error: Manifest inspection is not supported for this table type; expected a FileStoreTable.',
              file=sys.stderr)
        sys.exit(1)
    return table


def _manifest_entry_to_dict(table, entry):
    """Keep all stored file metadata and add the resolved data-file path and format."""
    from dataclasses import fields
    from pypaimon.table.system.files_table import _data_file_format, _metadata_file_path

    meta = entry.file
    file = {field.name: getattr(meta, field.name) for field in fields(meta)}
    if table.primary_keys and meta.schema_id != table.table_schema.id:
        schema = table.schema_manager.get_schema(meta.schema_id)
        key_names = [name for name in schema.primary_keys if name not in schema.partition_keys]
        # Key types cannot change, but the reader uses the latest key names.
        # Restore the names recorded by this file's schema after a rename.
        for name in ('min_key', 'max_key', 'key_stats'):
            file[name] = _manifest_json_value(file[name], key_names)
    file['file_path'] = _metadata_file_path(table, entry)
    file['file_format'] = _data_file_format(meta.file_name)
    file['file_source'] = {0: 'APPEND', 1: 'COMPACT'}.get(meta.file_source, meta.file_source)
    return {
        'kind': {0: 'ADD', 1: 'DELETE'}.get(entry.kind, entry.kind),
        'partition': entry.partition,
        'bucket': entry.bucket,
        'total_buckets': entry.total_buckets,
        'file': file,
    }


def _manifest_json_value(value, field_names=None):
    """Render decoded metadata without losing integers, decimal precision or binary values."""
    import base64
    import math
    from datetime import date, time
    from decimal import Decimal
    from pypaimon.data.timestamp import Timestamp
    from pypaimon.manifest.schema.simple_stats import SimpleStats
    from pypaimon.table.row.internal_row import InternalRow

    if isinstance(value, InternalRow):
        names = [field.name for field in value.fields] if field_names is None else field_names
        # Empty stats can retain schema fields even when the stored row has no values.
        return {name: _manifest_json_value(value.get_field(i)) for i, name in enumerate(names[:len(value)])}
    if isinstance(value, SimpleStats):
        counts = value.null_counts
        names = [field.name for field in value.min_values.fields] if field_names is None else field_names
        return {
            'min_values': _manifest_json_value(value.min_values, names),
            'max_values': _manifest_json_value(value.max_values, names),
            'null_counts': None if counts is None else {
                name: counts[i] if i < len(counts) else None for i, name in enumerate(names)
            },
        }
    if isinstance(value, dict):
        return {key: _manifest_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_manifest_json_value(item) for item in value]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {'encoding': 'base64', 'data': base64.b64encode(value).decode('ascii')}
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Timestamp):
        return value.to_local_date_time().isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return 'NaN' if math.isnan(value) else ('Infinity' if value > 0 else '-Infinity')
    return value


def add_table_subcommands(table_parser):
    """
    Add table subcommands to the parser.
    
    Args:
        table_parser: The table subparser to add commands to.
    """
    table_subparsers = table_parser.add_subparsers(dest='table_command', help='Table commands')
    
    # table read command
    read_parser = table_subparsers.add_parser('read', help='Read data from a table')
    read_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    read_parser.add_argument(
        '--select', '-s',
        type=str,
        default=None,
        help='Select specific columns to read (comma-separated, e.g., "id,name,age")'
    )
    read_parser.add_argument(
        '--where', '-w',
        type=str,
        default=None,
        help=('Filter condition in SQL-like syntax '
              '(e.g., "age > 18", "name = \'Alice\' AND status IN (\'active\', \'pending\')")')
    )
    read_parser.add_argument(
        '--limit', '-l',
        type=int,
        default=100,
        help='Maximum number of results to display (default: 100)'
    )
    read_parser.add_argument(
        '--format', '-f',
        type=str,
        choices=['table', 'json'],
        default='table',
        help='Output format: table (default) or json'
    )
    read_parser.set_defaults(func=cmd_table_read)

    # table explain command
    explain_parser = table_subparsers.add_parser(
        'explain',
        help='Show the scan plan (snapshot, pushdown, pruning funnel, split shape) '
             'without reading data'
    )
    explain_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    explain_parser.add_argument(
        '--select', '-s',
        type=str,
        default=None,
        help='Project specific columns (comma-separated, e.g., "id,name,age")'
    )
    explain_parser.add_argument(
        '--where', '-w',
        type=str,
        default=None,
        help='Filter condition in SQL-like syntax '
             '(e.g., "age > 18", "dt = \'2026-01-01\' AND id IN (1,2,3)")'
    )
    explain_parser.add_argument(
        '--limit', '-l',
        type=int,
        default=None,
        help='Row limit to push down'
    )
    explain_parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='List every split with its files'
    )
    explain_parser.add_argument(
        '--format', '-f',
        type=str,
        choices=['table', 'json'],
        default='table',
        help='Output format: table (default) or json'
    )
    explain_parser.set_defaults(func=cmd_table_explain)

    # table get command
    get_parser = table_subparsers.add_parser('get', help='Get table schema information')
    get_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    schema_target = get_parser.add_mutually_exclusive_group()
    schema_target.add_argument('--schema-id', type=_non_negative_int, default=None,
                               help='Schema ID to inspect (default: the latest schema)')
    schema_target.add_argument('--snapshot-id', type=_positive_int, default=None,
                               help='Inspect the schema referenced by this snapshot')
    get_parser.add_argument(
        '--format', '-f', choices=['table', 'json'], default='json',
        help='Output format: json (default) or table'
    )
    get_parser.set_defaults(func=cmd_table_get)

    # table list-schemas command
    list_schemas_parser = table_subparsers.add_parser(
        'list-schemas', help='List available schemas in ascending schema-ID order'
    )
    list_schemas_parser.add_argument('table', help='Table identifier in format: database.table')
    list_schemas_parser.add_argument(
        '--limit', '-l', type=_positive_int, default=None,
        help='Maximum number of schemas to display, starting with the oldest (default: all)'
    )
    list_schemas_parser.add_argument(
        '--format', '-f', choices=['table', 'json'], default='json',
        help='Output format: json (default) or table'
    )
    list_schemas_parser.set_defaults(func=cmd_table_list_schemas)
    
    # table snapshot command
    snapshot_parser = table_subparsers.add_parser('snapshot', help='Get the latest or a specific snapshot of a table')
    snapshot_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    snapshot_parser.add_argument(
        '--snapshot-id', type=_positive_int, default=None,
        help='Snapshot ID to inspect (default: the latest snapshot)'
    )
    snapshot_parser.add_argument(
        '--format', '-f', choices=['table', 'json'], default='json',
        help='Output format: json (default) or table'
    )
    snapshot_parser.set_defaults(func=cmd_table_snapshot)

    # table list-snapshots command
    list_snapshots_parser = table_subparsers.add_parser(
        'list-snapshots', help='List available snapshots in ascending snapshot-ID order'
    )
    list_snapshots_parser.add_argument('table', help='Table identifier in format: database.table')
    list_snapshots_parser.add_argument(
        '--limit', '-l', type=_positive_int, default=None,
        help='Maximum number of snapshots to display, starting with the oldest (default: all)'
    )
    list_snapshots_parser.add_argument(
        '--format', '-f', choices=['table', 'json'], default='table',
        help='Output format: table (default) or json'
    )
    list_snapshots_parser.set_defaults(func=cmd_table_list_snapshots)

    # table list-manifests command
    list_manifests_parser = table_subparsers.add_parser(
        'list-manifests', help='List the manifests referenced by a snapshot'
    )
    list_manifests_parser.add_argument('table', help='Table identifier in format: database.table')
    list_manifests_parser.add_argument(
        '--snapshot-id', type=_positive_int, default=None,
        help='Snapshot ID to inspect (default: the latest snapshot)'
    )
    list_manifests_parser.add_argument(
        '--type', choices=['all', 'base', 'delta', 'changelog'], default='all',
        help='Manifest list type (default: all)'
    )
    list_manifests_parser.add_argument(
        '--limit', '-l', type=_positive_int, default=None,
        help='Maximum number of manifests across all selected lists (default: all)'
    )
    list_manifests_parser.add_argument(
        '--format', '-f', choices=['table', 'json'], default='table',
        help='Output format: table (default) or json'
    )
    list_manifests_parser.set_defaults(func=cmd_table_list_manifests)

    # table manifest command
    manifest_parser = table_subparsers.add_parser(
        'manifest', help='Inspect a manifest and its original ADD/DELETE entries'
    )
    manifest_parser.add_argument('table', help='Table identifier in format: database.table')
    manifest_parser.add_argument('--file', required=True, help='Manifest file name within the table manifest directory')
    manifest_parser.add_argument(
        '--format', '-f', choices=['table', 'json'], default='json',
        help='Output format: json (default) or table'
    )
    manifest_parser.set_defaults(func=cmd_table_manifest)

    # table list-files command
    list_files_parser = table_subparsers.add_parser('list-files', help='List the live data files of a snapshot')
    list_files_parser.add_argument('table', help='Table identifier in format: database.table')
    list_files_parser.add_argument(
        '--snapshot-id', type=_positive_int, default=None,
        help='Snapshot ID to inspect (default: the latest snapshot)'
    )
    list_files_parser.add_argument('--bucket', type=int, default=None, help='Only display files in this bucket')
    list_files_parser.add_argument(
        '--limit', '-l', type=_positive_int, default=None,
        help='Maximum number of live files to display after filtering (default: all)'
    )
    list_files_parser.add_argument(
        '--format', '-f', choices=['table', 'json'], default='table',
        help='Output format: table (default) or json'
    )
    list_files_parser.set_defaults(func=cmd_table_list_files)

    # table file command
    file_parser = table_subparsers.add_parser('file', help="Inspect a Parquet file's schema and footer metadata")
    file_parser.add_argument('table', help='Table identifier in format: database.table (provides FileIO context)')
    file_parser.add_argument('--file', required=True, help='Actual Parquet file path or filesystem URI')
    file_parser.add_argument(
        '--format', '-f', choices=['table', 'json'], default='json',
        help='Output format: json (default) or table'
    )
    file_parser.set_defaults(func=cmd_table_file)

    # table rollback command
    rollback_parser = table_subparsers.add_parser(
        'rollback',
        help='Roll back a table to an earlier snapshot, tag, or timestamp')
    rollback_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    rollback_target = rollback_parser.add_mutually_exclusive_group(required=True)
    rollback_target.add_argument(
        '--snapshot-id', type=int, default=None,
        help='Snapshot ID to roll back to'
    )
    rollback_target.add_argument(
        '--tag', type=str, default=None,
        help='Tag name to roll back to'
    )
    rollback_target.add_argument(
        '--timestamp', type=int, default=None,
        help='Epoch milliseconds; roll back to the latest snapshot at or before it'
    )
    rollback_parser.set_defaults(func=cmd_table_rollback)

    # table create command
    create_parser = table_subparsers.add_parser('create', help='Create a new table')
    create_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    create_parser.add_argument(
        '--schema', '-s',
        required=True,
        help='Path to schema JSON file'
    )
    create_parser.add_argument(
        '--ignore-if-exists', '-i',
        action='store_true',
        help='Do not raise error if table already exists'
    )
    create_parser.set_defaults(func=cmd_table_create)
    
    # table drop command
    drop_parser = table_subparsers.add_parser('drop', help='Drop a table')
    drop_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    drop_parser.add_argument(
        '--ignore-if-not-exists', '-i',
        action='store_true',
        help='Do not raise error if table does not exist'
    )
    drop_parser.set_defaults(func=cmd_table_drop)
    
    # table drop-partition command
    drop_partition_parser = table_subparsers.add_parser(
        'drop-partition', help='Drop one or more partitions from a table')
    drop_partition_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    drop_partition_parser.add_argument(
        '--partition', '-p',
        action='append',
        required=True,
        help=('Partition spec in format: "key1=value1,key2=value2". '
              'Can be specified multiple times to drop multiple partitions.')
    )
    drop_partition_parser.set_defaults(func=cmd_table_drop_partition)

    # table import command
    import_parser = table_subparsers.add_parser('import', help='Import data from CSV or JSON file')
    import_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    import_parser.add_argument(
        '--input', '-i',
        required=True,
        help='Path to input file (CSV or JSON format)'
    )
    import_parser.set_defaults(func=cmd_table_import)
    
    # table list-partitions command
    list_partitions_parser = table_subparsers.add_parser('list-partitions', help='List partitions of a table')
    list_partitions_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    list_partitions_parser.add_argument(
        '--pattern', '-p',
        type=str,
        default=None,
        help='Partition name pattern to filter partitions (e.g., "dt=2024*")'
    )
    list_partitions_parser.add_argument(
        '--format', '-f',
        type=str,
        choices=['table', 'json'],
        default='table',
        help='Output format: table (default) or json'
    )
    list_partitions_parser.set_defaults(func=cmd_table_list_partitions)

    # table rename command
    rename_parser = table_subparsers.add_parser('rename', help='Rename a table')

    # table full-text-search command
    fts_parser = table_subparsers.add_parser('full-text-search', help='Full-text search on a table')
    fts_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    fts_parser.add_argument(
        '--column',
        required=True,
        help='Text column to search'
    )
    fts_parser.add_argument(
        '--query', '-q',
        required=True,
        help='Full-text query string to search for'
    )
    fts_parser.add_argument(
        '--limit', '-l',
        type=int,
        default=10,
        help='Maximum number of results to return (default: 10)'
    )
    fts_parser.add_argument(
        '--select', '-s',
        type=str,
        default=None,
        help='Select specific columns to display (comma-separated, e.g., "id,name,content")'
    )
    fts_parser.add_argument(
        '--format', '-f',
        type=str,
        choices=['table', 'json'],
        default='table',
        help='Output format: table (default) or json'
    )
    fts_parser.set_defaults(func=cmd_table_full_text_search)
    rename_parser.add_argument(
        'table',
        help='Source table identifier in format: database.table'
    )
    rename_parser.add_argument(
        'target',
        help='Target table identifier in format: database.table'
    )
    rename_parser.set_defaults(func=cmd_table_rename)

    # table alter command
    alter_parser = table_subparsers.add_parser('alter', help='Alter a table with schema changes')
    alter_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    alter_parser.add_argument(
        '--ignore-if-not-exists', '-i',
        action='store_true',
        help='Do not raise error if table does not exist'
    )
    alter_subparsers = alter_parser.add_subparsers(dest='alter_command', help='Alter commands')
    
    # alter set-option
    set_option_parser = alter_subparsers.add_parser('set-option', help='Set a table option')
    set_option_parser.add_argument('--key', '-k', required=True, help='Option key')
    set_option_parser.add_argument('--value', '-v', required=True, help='Option value')
    set_option_parser.set_defaults(func=cmd_table_alter)
    
    # alter remove-option
    remove_option_parser = alter_subparsers.add_parser('remove-option', help='Remove a table option')
    remove_option_parser.add_argument('--key', '-k', required=True, help='Option key to remove')
    remove_option_parser.set_defaults(func=cmd_table_alter)
    
    # alter add-column
    add_column_parser = alter_subparsers.add_parser('add-column', help='Add a column to the table')
    add_column_parser.add_argument('--name', '-n', required=True, help='Column name')
    add_column_parser.add_argument('--type', '-t', required=True, help='Column data type (e.g. INT, STRING, BIGINT)')
    add_column_parser.add_argument('--comment', '-c', default=None, help='Column comment')
    add_column_position = add_column_parser.add_mutually_exclusive_group()
    add_column_position.add_argument('--first', action='store_true', help='Add column as the first column')
    add_column_position.add_argument('--after', metavar='COLUMN', help='Add column after the specified column')
    add_column_parser.set_defaults(func=cmd_table_alter)
    
    # alter drop-column
    drop_column_parser = alter_subparsers.add_parser('drop-column', help='Drop a column from the table')
    drop_column_parser.add_argument('--name', '-n', required=True, help='Column name to drop')
    drop_column_parser.set_defaults(func=cmd_table_alter)
    
    # alter rename-column
    rename_column_parser = alter_subparsers.add_parser('rename-column', help='Rename a column')
    rename_column_parser.add_argument('--name', '-n', required=True, help='Current column name')
    rename_column_parser.add_argument('--new-name', '-m', required=True, help='New column name')
    rename_column_parser.set_defaults(func=cmd_table_alter)
    
    # alter alter-column (change column type, comment, or position)
    alter_column_parser = alter_subparsers.add_parser('alter-column', help='Alter column type, comment, or position')
    alter_column_parser.add_argument('--name', '-n', required=True, help='Column name to alter')
    alter_column_parser.add_argument('--type', '-t', default=None, help='New column data type (e.g. DOUBLE, BIGINT)')
    alter_column_parser.add_argument('--comment', '-c', default=None, help='New column comment')
    alter_column_position = alter_column_parser.add_mutually_exclusive_group()
    alter_column_position.add_argument('--first', action='store_true', help='Move column to the first position')
    alter_column_position.add_argument('--after', metavar='COLUMN', help='Move column after the specified column')
    alter_column_parser.set_defaults(func=cmd_table_alter)
    
    # alter update-comment
    update_comment_parser = alter_subparsers.add_parser('update-comment', help='Update table comment')
    update_comment_parser.add_argument('--comment', '-c', required=True, help='New table comment')
    update_comment_parser.set_defaults(func=cmd_table_alter)


def _non_negative_int(value):
    """Argparse type for schema IDs, including the initial schema ID zero."""
    try:
        number = int(value)
        if number >= 0:
            return number
    except ValueError:
        pass
    raise argparse.ArgumentTypeError('must be a non-negative integer')


def _positive_int(value):
    """Argparse type for snapshot IDs and result limits."""
    try:
        number = int(value)
        if number > 0:
            return number
    except ValueError:
        pass
    raise argparse.ArgumentTypeError('must be a positive integer')


def _snapshot_commit_time(time_millis):
    # Keep millisecond precision and make the display independent of local TZ.
    instant = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=time_millis)
    return instant.isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def _snapshot_display_value(value):
    if value is None:
        return '-'
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)
