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

import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_exception import (DatabaseAlreadyExistException,
                                                DatabaseNotExistException,
                                                TableAlreadyExistException,
                                                TableNotExistException)
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.file_store_table import FileStoreTable


class FileSystemCatalogTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_")
        self.warehouse = os.path.join(self.temp_dir, 'test_dir')

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_database(self):
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db", False)
        self.assertTrue(os.path.exists(self.warehouse + "/test_db.db"))

        with self.assertRaises(DatabaseAlreadyExistException):
            catalog.create_database("test_db", False)

        catalog.create_database("test_db", True)

        with self.assertRaises(DatabaseNotExistException):
            catalog.get_database("test_db_x")

        database = catalog.get_database("test_db")
        self.assertEqual(database.name, "test_db")

    def test_cascade_drop_preserves_table_created_during_drop(self):
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("db", False)
        schema = Schema(fields=[DataField(0, "value", AtomicType("INT"))])
        catalog.create_table("db.old", schema, False)
        delete = catalog.file_io.delete
        old_path = catalog.get_database_path("db") + "/old"

        def delete_and_create(path, recursive=False):
            result = delete(path, recursive)
            if path == old_path:
                catalog.create_table("db.new", schema, False)
            return result

        with patch.object(catalog.file_io, "delete", side_effect=delete_and_create):
            with self.assertRaisesRegex(OSError, "changed during drop"):
                catalog.drop_database("db", cascade=True)

        self.assertEqual(["new"], catalog.list_tables("db"))
        self.assertIsNotNone(catalog.get_table("db.new"))
        catalog.drop_database("db", cascade=True)
        with self.assertRaises(DatabaseNotExistException):
            catalog.get_database("db")

    def test_table(self):
        fields = [
            DataField.from_dict({"id": 1, "name": "f0", "type": "INT"}),
            DataField.from_dict({"id": 2, "name": "f1", "type": "INT"}),
            DataField.from_dict({"id": 3, "name": "f2", "type": "STRING"}),
        ]
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db", False)
        catalog.create_table("test_db.test_table", Schema(fields=fields), False)
        self.assertTrue(os.path.exists(self.warehouse + "/test_db.db/test_table/schema/schema-0"))

        with self.assertRaises(TableAlreadyExistException):
            catalog.create_table("test_db.test_table", Schema(fields=fields), False)

        catalog.create_table("test_db.test_table", Schema(fields=fields), True)

        database = catalog.get_database("test_db")
        self.assertEqual(database.name, "test_db")

        with self.assertRaises(TableNotExistException):
            catalog.get_table("test_db.test_table_x")

        table = catalog.get_table("test_db.test_table")
        self.assertTrue(table is not None)
        self.assertTrue(isinstance(table, FileStoreTable))
        self.assertEqual(table.fields[2].name, "f2")
        self.assertTrue(isinstance(table.fields[2].type, AtomicType))
        self.assertEqual(table.fields[2].type.type, "STRING")

    def test_s3_filesystem_catalog_with_paimon_options(self):
        with patch("pypaimon.filesystem.pyarrow_file_io.pafs.S3FileSystem") as s3_file_system:
            CatalogFactory.create({
                "warehouse": "s3://bucket/warehouse",
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key": "access-key",
                "s3.secret-key": "secret-key",
                "s3.session-token": "session-token",
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
            })

        s3_file_system.assert_called_once()
        kwargs = s3_file_system.call_args[1]
        self.assertEqual(kwargs["endpoint_override"], "http://localhost:9000")
        self.assertEqual(kwargs["access_key"], "access-key")
        self.assertEqual(kwargs["secret_key"], "secret-key")
        self.assertEqual(kwargs["session_token"], "session-token")
        self.assertEqual(kwargs["region"], "us-east-1")
        if "force_virtual_addressing" in kwargs:
            self.assertFalse(kwargs["force_virtual_addressing"])

    def test_s3_filesystem_catalog_with_legacy_options(self):
        with patch("pypaimon.filesystem.pyarrow_file_io.pafs.S3FileSystem") as s3_file_system:
            CatalogFactory.create({
                "warehouse": "s3://bucket/warehouse",
                "fs.s3.endpoint": "http://localhost:9000",
                "fs.s3.accessKeyId": "access-key",
                "fs.s3.accessKeySecret": "secret-key",
                "fs.s3.securityToken": "session-token",
                "fs.s3.region": "us-east-1",
            })

        s3_file_system.assert_called_once()
        kwargs = s3_file_system.call_args[1]
        self.assertEqual(kwargs["endpoint_override"], "http://localhost:9000")
        self.assertEqual(kwargs["access_key"], "access-key")
        self.assertEqual(kwargs["secret_key"], "secret-key")
        self.assertEqual(kwargs["session_token"], "session-token")
        self.assertEqual(kwargs["region"], "us-east-1")
        if "force_virtual_addressing" in kwargs:
            self.assertTrue(kwargs["force_virtual_addressing"])

    def test_alter_table(self):
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db", False)

        identifier = "test_db.test_table"
        schema = Schema(
            fields=[
                DataField.from_dict({"id": 0, "name": "col1", "type": "STRING", "description": "field1"}),
                DataField.from_dict({"id": 1, "name": "col2", "type": "STRING", "description": "field2"})
            ],
            partition_keys=[],
            primary_keys=[],
            options={},
            comment="comment"
        )
        catalog.create_table(identifier, schema, False)

        catalog.alter_table(
            identifier,
            [SchemaChange.add_column("col3", AtomicType("DATE"))],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 3)
        self.assertEqual(table.fields[2].name, "col3")
        self.assertEqual(table.fields[2].type.type, "DATE")

        catalog.alter_table(
            identifier,
            [SchemaChange.update_comment("new comment")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(table.table_schema.comment, "new comment")

        catalog.alter_table(
            identifier,
            [SchemaChange.rename_column("col1", "new_col1")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(table.fields[0].name, "new_col1")

        catalog.alter_table(
            identifier,
            [SchemaChange.update_column_type("col2", AtomicType("BIGINT"))],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(table.fields[1].type.type, "BIGINT")

        catalog.alter_table(
            identifier,
            [SchemaChange.update_column_comment("col2", "col2 field")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(table.fields[1].description, "col2 field")

        catalog.alter_table(
            identifier,
            [SchemaChange.set_option("write-buffer-size", "256 MB")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(table.table_schema.options.get("write-buffer-size"), "256 MB")

        catalog.alter_table(
            identifier,
            [SchemaChange.remove_option("write-buffer-size")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertNotIn("write-buffer-size", table.table_schema.options)

        catalog.alter_table(
            identifier,
            [SchemaChange.drop_column("col3")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 2)

    def test_alter_immutable_options(self):
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        # while a table has no snapshots, structural options may still be changed
        empty_identifier = "test_db.immutable_options_empty"
        pa_schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
        catalog.create_table(empty_identifier, Schema.from_pyarrow_schema(pa_schema), False)
        catalog.alter_table(
            empty_identifier,
            [SchemaChange.set_option("merge-engine", "first-row")],
            False
        )
        # the type is rejected even without snapshots: table kinds like format
        # tables never create Paimon snapshots but can still hold data
        with self.assertRaises(RuntimeError) as ctx:
            catalog.alter_table(
                empty_identifier,
                [SchemaChange.set_option("type", "format-table")], False)
        self.assertIn("Change 'type' is not supported yet.", str(ctx.exception))
        with self.assertRaises(RuntimeError):
            catalog.alter_table(
                empty_identifier, [SchemaChange.remove_option("type")], False)

        # once the table has a snapshot, immutable options are rejected
        identifier = "test_db.immutable_options_table"
        catalog.create_table(identifier, Schema.from_pyarrow_schema(pa_schema), False)
        table = catalog.get_table(identifier)
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(
            pa.Table.from_pydict({'id': [1, 2], 'name': ['a', 'b']}, schema=pa_schema))
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # the catalog wraps alter failures in RuntimeError
        with self.assertRaises(RuntimeError) as ctx:
            catalog.alter_table(
                identifier, [SchemaChange.set_option("type", "format-table")], False)
        self.assertIn("Change 'type' is not supported yet.", str(ctx.exception))

        with self.assertRaises(RuntimeError) as ctx:
            catalog.alter_table(
                identifier, [SchemaChange.set_option("merge-engine", "deduplicate")], False)
        self.assertIn("Change 'merge-engine' is not supported yet.", str(ctx.exception))

        with self.assertRaises(RuntimeError) as ctx:
            catalog.alter_table(
                identifier, [SchemaChange.remove_option("merge-engine")], False)
        self.assertIn("Change 'merge-engine' is not supported yet.", str(ctx.exception))

        # canonical hyphenated keys are guarded too
        with self.assertRaises(RuntimeError) as ctx:
            catalog.alter_table(
                identifier,
                [SchemaChange.set_option("index-file-in-data-file-dir", "true")], False)
        self.assertIn(
            "Change 'index-file-in-data-file-dir' is not supported yet.", str(ctx.exception))

        for change, message in [
            (SchemaChange.set_option("bucket", "4"), "Cannot change bucket when it is -1."),
            (SchemaChange.remove_option("bucket"), "Cannot reset bucket."),
            (SchemaChange.set_option("deletion-vectors.enabled", "true"), "Cannot change deletion vectors mode"),
            (SchemaChange.set_option("bucket-function.provider", "custom"),
             "Change 'bucket-function.provider' is not supported yet."),
        ]:
            with self.assertRaises(RuntimeError) as ctx:
                catalog.alter_table(identifier, [change], False)
            self.assertIn(message, str(ctx.exception))
            self.assertEqual(catalog.get_table(identifier).table_schema.id, table.table_schema.id)

        # setting the default type explicitly is not a change
        catalog.alter_table(
            identifier, [SchemaChange.set_option("type", "table")], False)
        table = catalog.get_table(identifier)
        self.assertNotIn("type", table.table_schema.options)
        # ...and the reloaded effective type resolves to the requested "table"
        # (the default), not some other configured default
        from pypaimon.common.options import CoreOptions, Options
        self.assertEqual(
            CoreOptions(Options(table.table_schema.options)).type(), "table")

        # mutable options can still be changed
        catalog.alter_table(
            identifier, [SchemaChange.set_option("target-file-size", "10mb")], False)

        # repeated changes to the same option in one batch apply in order
        catalog.alter_table(identifier, [
            SchemaChange.set_option("target-file-size", "20mb"),
            SchemaChange.set_option("target-file-size", "10mb")], False)
        table = catalog.get_table(identifier)
        self.assertEqual(table.table_schema.options.get("target-file-size"), "10mb")
        catalog.alter_table(identifier, [
            SchemaChange.remove_option("target-file-size"),
            SchemaChange.set_option("target-file-size", "10mb")], False)
        table = catalog.get_table(identifier)
        self.assertEqual(table.table_schema.options.get("target-file-size"), "10mb")

        # replaying the actual primary/partition keys is not a change: they are
        # stored in the schema fields, not the options map
        pk_identifier = "test_db.immutable_options_pk"
        catalog.create_table(
            pk_identifier,
            Schema.from_pyarrow_schema(
                pa_schema, primary_keys=['id'], options={'bucket': '1'}),
            False)
        pk_table = catalog.get_table(pk_identifier)
        write_builder = pk_table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(
            pa.Table.from_pydict({'id': [1], 'name': ['a']}, schema=pa_schema))
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
        catalog.alter_table(
            pk_identifier, [SchemaChange.set_option("primary-key", "id")], False)
        pk_table = catalog.get_table(pk_identifier)
        self.assertNotIn("primary-key", pk_table.table_schema.options)
        with self.assertRaises(RuntimeError):
            catalog.alter_table(
                pk_identifier, [SchemaChange.set_option("primary-key", "name")], False)

        part_identifier = "test_db.immutable_options_part"
        self._create_partitioned_table_with_data(
            catalog, part_identifier, [{'dt': '2026-01-01', 'rows': 1}])
        catalog.alter_table(
            part_identifier, [SchemaChange.set_option("partition", "dt")], False)
        with self.assertRaises(RuntimeError):
            catalog.alter_table(
                part_identifier, [SchemaChange.set_option("partition", "col1")], False)

        # the LATEST file is only a hint: the guard must still see the
        # snapshots when it is missing
        os.remove(os.path.join(
            self.warehouse, "test_db.db", "immutable_options_table",
            "snapshot", "LATEST"))
        with self.assertRaises(RuntimeError):
            catalog.alter_table(
                identifier,
                [SchemaChange.set_option("merge-engine", "deduplicate")], False)

    def test_update_column_type_guards_null_to_not_null(self):
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db_guard", False)

        def _make_table(name, options):
            identifier = "test_db_guard.{}".format(name)
            schema = Schema(
                fields=[
                    DataField.from_dict({"id": 0, "name": "k", "type": "INT"}),
                    DataField.from_dict({"id": 1, "name": "v", "type": "BIGINT"}),
                ],
                partition_keys=[], primary_keys=[], options=options, comment="",
            )
            catalog.create_table(identifier, schema, False)
            return identifier

        # Default option (disabled=true) rejects nullable -> not null, mirroring
        # Java SchemaManager#updateColumnType.
        default_id = _make_table("default_opt", {})
        with self.assertRaises(RuntimeError) as ctx:
            catalog.alter_table(
                default_id,
                [SchemaChange.update_column_type(
                    "v", AtomicType("BIGINT", nullable=False))],
                False)
        self.assertIn("nullable to non nullable", str(ctx.exception))

        # Opting out via the table option allows the transition.
        allowed_id = _make_table(
            "allow_opt", {"alter-column-null-to-not-null.disabled": "false"})
        catalog.alter_table(
            allowed_id,
            [SchemaChange.update_column_type(
                "v", AtomicType("BIGINT", nullable=False))],
            False)
        table = catalog.get_table(allowed_id)
        self.assertFalse(table.fields[1].type.nullable)

    def test_update_column_type_rejects_non_executable_cast(self):
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db_cast", False)

        identifier = "test_db_cast.time_table"
        schema = Schema(
            fields=[
                DataField.from_dict({"id": 0, "name": "k", "type": "INT"}),
                DataField.from_dict({"id": 1, "name": "t", "type": "TIME(3)"}),
            ],
            partition_keys=[], primary_keys=[], options={}, comment="",
        )
        catalog.create_table(identifier, schema, False)

        # TIME precision changes have no executor in either reader backend.
        with self.assertRaisesRegex(RuntimeError, "no executable cast"):
            catalog.alter_table(
                identifier,
                [SchemaChange.update_column_type("t", AtomicType("TIME(1)"))],
                False)

        # INT -> DECIMAL(12, 2) is executable in both reader backends.
        catalog.alter_table(
            identifier,
            [SchemaChange.update_column_type("k", AtomicType("DECIMAL(12, 2)"))],
            False)
        table = catalog.get_table(identifier)
        self.assertEqual(table.fields[0].type.type, "DECIMAL(12, 2)")

    def test_update_column_type_parameterized_not_null_target(self):
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db_nn_param", False)
        identifier = "test_db_nn_param.t"
        schema = Schema(
            fields=[
                DataField.from_dict({"id": 0, "name": "v", "type": "INT NOT NULL"}),
                DataField.from_dict({"id": 1, "name": "s", "type": "STRING"}),
            ],
            partition_keys=[], primary_keys=[], options={}, comment="",
        )
        catalog.create_table(identifier, schema, False)

        # Widening a non-null INT to a non-null DECIMAL(12, 2) is valid. The
        # target's to_dict() is "DECIMAL(12, 2) NOT NULL"; the atomic parser must
        # keep the nullability in `nullable` so the executable-cast check does
        # not choke on a doubled "NOT NULL NOT NULL" type string.
        catalog.alter_table(
            identifier,
            [SchemaChange.update_column_type(
                "v", AtomicType("DECIMAL(12, 2)", nullable=False))],
            False)
        table = catalog.get_table(identifier)
        self.assertEqual(table.fields[0].type.type, "DECIMAL(12, 2)")
        self.assertFalse(table.fields[0].type.nullable)

    def test_add_column_before_partition(self):
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db", False)

        identifier = "test_db.test_table"
        schema = Schema(
            fields=[
                DataField.from_dict({"id": 0, "name": "col1", "type": "STRING", "description": "field1"}),
                DataField.from_dict(
                    {"id": 1, "name": "partition_col", "type": "STRING", "description": "partition field"})
            ],
            partition_keys=["partition_col"],
            primary_keys=[],
            options={},
            comment="comment"
        )
        catalog.create_table(identifier, schema, False)

        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 2)
        self.assertEqual(table.fields[1].name, "partition_col")

        catalog.alter_table(
            identifier,
            [SchemaChange.set_option("add-column-before-partition", "true")],
            False
        )

        catalog.alter_table(
            identifier,
            [SchemaChange.add_column("new_col", AtomicType("INT"))],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 3)
        self.assertEqual(table.fields[0].name, "col1")
        self.assertEqual(table.fields[1].name, "new_col")
        self.assertEqual(table.fields[2].name, "partition_col")

        catalog.alter_table(
            identifier,
            [SchemaChange.add_column("col_multi1", AtomicType("INT")),
             SchemaChange.add_column("col_multi2", AtomicType("STRING")),
             SchemaChange.add_column("col_multi3", AtomicType("DOUBLE"))],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 6)
        self.assertEqual(table.fields[0].name, "col1")
        self.assertEqual(table.fields[1].name, "new_col")
        self.assertEqual(table.fields[2].name, "col_multi1")
        self.assertEqual(table.fields[3].name, "col_multi2")
        self.assertEqual(table.fields[4].name, "col_multi3")
        self.assertEqual(table.fields[5].name, "partition_col")

        catalog.alter_table(
            identifier,
            [SchemaChange.set_option("add-column-before-partition", "false")],
            False
        )

        catalog.alter_table(
            identifier,
            [SchemaChange.add_column("another_col", AtomicType("BIGINT"))],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 7)
        self.assertEqual(table.fields[0].name, "col1")
        self.assertEqual(table.fields[1].name, "new_col")
        self.assertEqual(table.fields[2].name, "col_multi1")
        self.assertEqual(table.fields[3].name, "col_multi2")
        self.assertEqual(table.fields[4].name, "col_multi3")
        self.assertEqual(table.fields[5].name, "partition_col")
        self.assertEqual(table.fields[6].name, "another_col")

    def test_get_database_propagates_exists_error(self):
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })

        with self.assertRaises(DatabaseNotExistException):
            catalog.get_database("nonexistent_db")

        catalog.create_database("test_db", False)

        # FileSystemCatalog has file_io attribute
        from pypaimon.catalog.filesystem_catalog import FileSystemCatalog
        self.assertIsInstance(catalog, FileSystemCatalog)
        filesystem_catalog = catalog  # type: FileSystemCatalog

        original_exists = filesystem_catalog.file_io.exists
        filesystem_catalog.file_io.exists = MagicMock(side_effect=OSError("Permission denied"))

        # Now get_database should propagate OSError, not DatabaseNotExistException
        with self.assertRaises(OSError) as context:
            catalog.get_database("test_db")
        self.assertIn("Permission denied", str(context.exception))
        self.assertNotIsInstance(context.exception, DatabaseNotExistException)

        # Restore original method
        filesystem_catalog.file_io.exists = original_exists

    def _create_partitioned_table_with_data(self, catalog, identifier, partitions_data):
        pa_schema = pa.schema([
            ('dt', pa.string()),
            ('col1', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'])
        catalog.create_table(identifier, schema, True)
        table = catalog.get_table(identifier)

        for part in partitions_data:
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({
                'dt': [part['dt']] * part['rows'],
                'col1': list(range(part['rows'])),
            }, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

    def test_list_partitions_null_values(self):
        from datetime import date

        from pypaimon.common.options.core_options import CoreOptions

        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)
        cases = [
            (pa.date32(), date(2026, 8, 26), "2026-08-26"),
            (pa.string(), "None", "None"),
        ]
        for default_name in [None, "custom_null"]:
            for data_type, value, text in cases:
                with self.subTest(default_name=default_name, data_type=data_type):
                    identifier = "test_db.null_parts_{}_{}".format(
                        default_name or "default", "date" if pa.types.is_date32(data_type) else "string")
                    pa_schema = pa.schema([('dt', data_type), ('value', pa.int32())])
                    options = {"partition.legacy-name": "false"}
                    if default_name is not None:
                        options["partition.default-name"] = default_name
                    schema = Schema.from_pyarrow_schema(
                        pa_schema, partition_keys=['dt'], options=options)
                    catalog.create_table(identifier, schema, False)
                    table = catalog.get_table(identifier)
                    builder = table.new_batch_write_builder()
                    writer = builder.new_write()
                    committer = builder.new_commit()
                    try:
                        writer.write_arrow(pa.Table.from_pydict(
                            {'dt': [None, value], 'value': [1, 2]}, schema=pa_schema))
                        committer.commit(writer.prepare_commit())
                    finally:
                        writer.close()
                        committer.close()
                    null_name = default_name or CoreOptions.PARTITION_DEFAULT_NAME.default_value()
                    partitions = catalog.list_partitions_paged(identifier).elements
                    self.assertEqual(
                        {p.spec['dt']: p.record_count for p in partitions},
                        {null_name: 1, text: 1})
                    filtered = catalog.list_partitions_paged(
                        identifier, partition_name_pattern='dt=' + null_name)
                    self.assertEqual([p.spec for p in filtered.elements], [{'dt': null_name}])
                    catalog.drop_table(identifier)

    def test_list_and_drop_nanosecond_partitions(self):
        import pandas as pd

        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)
        for legacy in [True, False]:
            with self.subTest(legacy=legacy):
                identifier = "test_db.timestamp_{}".format(legacy)
                arrow_schema = pa.schema([('dt', pa.timestamp('ns')), ('value', pa.int32())])
                catalog.create_table(identifier, Schema.from_pyarrow_schema(
                    arrow_schema, partition_keys=['dt'],
                    options={"partition.legacy-name": str(legacy).lower()}), False)
                table = catalog.get_table(identifier)
                base = pd.Timestamp('2024-01-02 03:04:05')
                values = [base + pd.Timedelta(n, unit='ns') for n in range(3)]
                builder = table.new_batch_write_builder()
                writer = builder.new_write()
                committer = builder.new_commit()
                try:
                    writer.write_arrow(pa.Table.from_pydict({
                        'dt': [values[0], values[1], values[1], values[2], values[2], values[2]],
                        'value': list(range(6)),
                    }, schema=arrow_schema))
                    committer.commit(writer.prepare_commit())
                finally:
                    writer.close()
                    committer.close()
                partitions = catalog.list_partitions_paged(identifier).elements
                self.assertEqual(len(partitions), 3)
                self.assertEqual(len({p.spec['dt'] for p in partitions}), 3)
                target = next(p for p in partitions if p.record_count == 2)
                self.assertTrue(target.spec['dt'].endswith('.000000001'))
                catalog.drop_partitions(identifier, [target.spec])
                remaining = catalog.list_partitions_paged(identifier).elements
                self.assertEqual(sorted(p.record_count for p in remaining), [1, 3])
                self.assertEqual({p.spec['dt'] for p in remaining},
                                 {p.spec['dt'] for p in partitions if p.record_count != 2})

    def test_list_partitions_preserves_colliding_specs(self):
        from pypaimon.api.rest_exception import NotImplementedException
        from pypaimon.catalog.rest.rest_catalog import RESTCatalog
        from pypaimon.common.options.core_options import CoreOptions

        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)
        for default_name in [CoreOptions.PARTITION_DEFAULT_NAME.default_value(), "custom_null"]:
            with self.subTest(default_name=default_name):
                identifier = "test_db.collisions_" + default_name
                pa_schema = pa.schema([('dt', pa.string()), ('value', pa.int32())])
                schema = Schema.from_pyarrow_schema(
                    pa_schema, partition_keys=['dt'], options={
                        "partition.default-name": default_name,
                        "partition.legacy-name": "false"})
                catalog.create_table(identifier, schema, False)
                table = catalog.get_table(identifier)
                for _ in range(2):
                    builder = table.new_batch_write_builder()
                    writer = builder.new_write()
                    committer = builder.new_commit()
                    try:
                        writer.write_arrow(pa.Table.from_pydict({
                            'dt': [None, " ", " ", default_name, default_name, default_name],
                            'value': list(range(6))}, schema=pa_schema))
                        committer.commit(writer.prepare_commit())
                    finally:
                        writer.close()
                        committer.close()

                rest_catalog = object.__new__(RESTCatalog)
                rest_catalog.rest_api = MagicMock()
                rest_catalog.rest_api.list_partitions_paged.side_effect = NotImplementedException(
                    "partition listing not implemented")
                rest_catalog.get_table = MagicMock(return_value=table)
                for reader in [catalog, rest_catalog]:
                    partitions = reader.list_partitions_paged(identifier).elements
                    self.assertEqual(len(partitions), 3)
                    self.assertEqual(sorted(p.record_count for p in partitions), [2, 4, 6])
                    self.assertTrue(all(p.spec == {'dt': default_name} for p in partitions))
                    self.assertTrue(all(p.file_count == 2 for p in partitions))
                    from pypaimon.manifest.manifest_file_manager import ManifestFileManager
                    from pypaimon.manifest.manifest_list_manager import ManifestListManager

                    manifests = ManifestListManager(table).read_all(
                        table.snapshot_manager().get_latest_snapshot())
                    entries = ManifestFileManager(table).read_entries_parallel(manifests, drop_stats=True)
                    reordered = list(reversed(entries))
                    counts = []
                    token = None
                    with patch.object(ManifestFileManager, 'read_entries_parallel',
                                      side_effect=[entries, reordered, reordered]):
                        while True:
                            page = reader.list_partitions_paged(
                                identifier, max_results=1, page_token=token,
                                partition_name_pattern='dt=' + default_name)
                            counts.extend(p.record_count for p in page.elements)
                            token = page.next_page_token
                            if token is None:
                                break
                    self.assertEqual(counts, [p.record_count for p in partitions])

    def test_list_partitions_paged_pagination(self):
        """Test list_partitions_paged pagination with max_results and page_token."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        identifier = "test_db.paged_tbl"
        self._create_partitioned_table_with_data(catalog, identifier, [
            {'dt': '2024-01-01', 'rows': 1},
            {'dt': '2024-01-02', 'rows': 1},
            {'dt': '2024-01-03', 'rows': 1},
        ])

        # First page: max_results=2
        page1 = catalog.list_partitions_paged(identifier, max_results=2)
        self.assertEqual(len(page1.elements), 2)
        self.assertIsNotNone(page1.next_page_token)

        # Second page: use next_page_token
        page2 = catalog.list_partitions_paged(
            identifier, max_results=2, page_token=page1.next_page_token
        )
        self.assertEqual(len(page2.elements), 1)
        self.assertIsNone(page2.next_page_token)

        # All specs across pages should cover all 3 partitions
        all_specs = [p.spec['dt'] for p in page1.elements + page2.elements]
        self.assertEqual(sorted(all_specs), ['2024-01-01', '2024-01-02', '2024-01-03'])

        # max_results larger than total returns all
        result = catalog.list_partitions_paged(identifier, max_results=100)
        self.assertEqual(len(result.elements), 3)
        self.assertIsNone(result.next_page_token)

    def test_list_partitions_paged_pattern(self):
        """Test list_partitions_paged with partition_name_pattern filter."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        identifier = "test_db.pattern_tbl"
        self._create_partitioned_table_with_data(catalog, identifier, [
            {'dt': '2024-01-01', 'rows': 1},
            {'dt': '2024-02-01', 'rows': 1},
            {'dt': '2024-02-15', 'rows': 1},
        ])

        # Exact match
        result = catalog.list_partitions_paged(
            identifier, partition_name_pattern='dt=2024-01-01'
        )
        self.assertEqual(len(result.elements), 1)
        self.assertEqual(result.elements[0].spec['dt'], '2024-01-01')

        # Wildcard match
        result = catalog.list_partitions_paged(
            identifier, partition_name_pattern='dt=2024-02*'
        )
        self.assertEqual(len(result.elements), 2)
        specs = sorted(p.spec['dt'] for p in result.elements)
        self.assertEqual(specs, ['2024-02-01', '2024-02-15'])

        # No match
        result = catalog.list_partitions_paged(
            identifier, partition_name_pattern='dt=2025*'
        )
        self.assertEqual(len(result.elements), 0)

    def test_drop_partitions(self):
        """Test drop_partitions removes specified partitions from a table."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        identifier = "test_db.drop_part_tbl"
        self._create_partitioned_table_with_data(catalog, identifier, [
            {'dt': '2024-01-01', 'rows': 2},
            {'dt': '2024-01-02', 'rows': 3},
            {'dt': '2024-01-03', 'rows': 1},
        ])

        # Verify all 3 partitions exist
        result = catalog.list_partitions_paged(identifier)
        self.assertEqual(len(result.elements), 3)

        # Drop one partition
        catalog.drop_partitions(identifier, [{'dt': '2024-01-02'}])

        # Verify only 2 partitions remain
        result = catalog.list_partitions_paged(identifier)
        self.assertEqual(len(result.elements), 2)
        specs = sorted(p.spec['dt'] for p in result.elements)
        self.assertEqual(specs, ['2024-01-01', '2024-01-03'])

    def test_drop_partitions_multiple(self):
        """Test drop_partitions with multiple partitions at once."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        identifier = "test_db.drop_multi_tbl"
        self._create_partitioned_table_with_data(catalog, identifier, [
            {'dt': '2024-01-01', 'rows': 1},
            {'dt': '2024-01-02', 'rows': 1},
            {'dt': '2024-01-03', 'rows': 1},
        ])

        # Drop two partitions at once
        catalog.drop_partitions(identifier, [
            {'dt': '2024-01-01'},
            {'dt': '2024-01-03'},
        ])

        # Verify only 1 partition remains
        result = catalog.list_partitions_paged(identifier)
        self.assertEqual(len(result.elements), 1)
        self.assertEqual(result.elements[0].spec['dt'], '2024-01-02')

    def test_drop_partitions_empty_list_raises(self):
        """Test drop_partitions raises ValueError for empty partitions list."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        identifier = "test_db.drop_empty_tbl"
        self._create_partitioned_table_with_data(catalog, identifier, [
            {'dt': '2024-01-01', 'rows': 1},
        ])

        with self.assertRaises(ValueError):
            catalog.drop_partitions(identifier, [])

    def test_create_table_auto_create_boolean_parsing(self):
        fields = [DataField.from_dict({"id": 0, "name": "f0", "type": "INT"})]
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        # auto-create=false is a legal no-op (the default) and must not be rejected.
        catalog.create_table(
            "test_db.ac_false",
            Schema(fields=fields, options={"auto-create": "false"}), False)
        self.assertTrue(
            os.path.exists(self.warehouse + "/test_db.db/ac_false/schema/schema-0"))

        # auto-create=true stays unsupported by create_table and is rejected.
        with self.assertRaises(ValueError):
            catalog.create_table(
                "test_db.ac_true",
                Schema(fields=fields, options={"auto-create": "true"}), False)
