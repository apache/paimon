/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.TableType;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.IoCacheViewableFileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TableFileIOView;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.iceberg.IcebergTable;
import org.apache.paimon.table.lance.LanceTable;
import org.apache.paimon.table.object.ObjectTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that each table takes its io-cache view of the catalog FileIO. */
class TableFileIOViewTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testTablesTakeTheirViewOfTheCatalogFileIO() throws Exception {
        ViewFileIO fileIO = new ViewFileIO(false, TableFileIOView.NONE);
        try (FileSystemCatalog catalog =
                new FileSystemCatalog(fileIO, new Path(tempDir.toUri().toString()))) {
            catalog.createDatabase("db", false);
            // this catalog only loads Format Tables, so write the schema directly
            Identifier format = Identifier.create("db", "format");
            new FileSystemSchemaManager(fileIO, catalog.getTableLocation(format))
                    .createTable(
                            Schema.newBuilder()
                                    .column("id", DataTypes.INT())
                                    .option(
                                            CoreOptions.TYPE.key(),
                                            TableType.FORMAT_TABLE.toString())
                                    .build());
            Identifier custom = Identifier.create("db", "custom");
            catalog.createTable(
                    custom,
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .option(CoreOptions.DATA_FILE_PREFIX.key(), "custom-")
                            .option(CoreOptions.CHANGELOG_FILE_PREFIX.key(), "cl-")
                            .build(),
                    false);
            Identifier plain = Identifier.create("db", "plain");
            catalog.createTable(
                    plain, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);

            // other writers may replace Format Table files in place
            Table formatTable = catalog.getTable(format);
            assertThat(viewOf(formatTable).originOnly).isTrue();
            assertThat(formatTable.copy(Collections.emptyMap()).fileIO())
                    .isSameAs(formatTable.fileIO());

            // a Paimon table owns its location and passes the file prefixes of its schema
            FileStoreTable table = (FileStoreTable) catalog.getTable(custom);
            TableFileIOView view = viewOf(table).view;
            assertThat(viewOf(table).originOnly).isFalse();
            assertThat(view.roots()).containsExactly(root(table.location()));
            assertThat(view.filePrefixes()).containsExactly("custom-", "cl-");
            assertThat(view.history())
                    .isEqualTo(
                            new TableFileIOViews.SchemaFilePrefixHistory(table.location(), "main"));
            FileStoreTable copy =
                    table.copy(
                            Collections.singletonMap(
                                    CoreOptions.DATA_FILE_PREFIX.key(), "dynamic-"));
            assertThat(viewOf(copy).view.filePrefixes())
                    .containsExactly("custom-", "cl-", "dynamic-");
            assertThat(table.copy(Collections.emptyMap()).fileIO()).isSameAs(table.fileIO());
            assertThat(table.copyWithLatestSchema().fileIO()).isSameAs(table.fileIO());

            FileStoreTable plainTable = (FileStoreTable) catalog.getTable(plain);
            assertThat(viewOf(plainTable).view.roots())
                    .containsExactly(root(plainTable.location()));
            assertThat(viewOf(plainTable).view.filePrefixes()).isEmpty();
        }

        ObjectTable objectTable =
                ObjectTable.builder()
                        .fileIO(fileIO)
                        .identifier(Identifier.create("db", "objects"))
                        .location(tempDir.toUri().toString())
                        .options(Collections.emptyMap())
                        .build();
        assertThat(viewOf(objectTable).originOnly).isTrue();
        IcebergTable icebergTable =
                IcebergTable.builder()
                        .fileIO(fileIO)
                        .identifier(Identifier.create("db", "iceberg"))
                        .rowType(RowType.of(DataTypes.INT()))
                        .partitionKeys(Collections.emptyList())
                        .location(tempDir.toUri().toString())
                        .options(Collections.emptyMap())
                        .build();
        assertThat(viewOf(icebergTable).originOnly).isTrue();
        LanceTable lanceTable =
                LanceTable.builder()
                        .fileIO(fileIO)
                        .identifier(Identifier.create("db", "lance"))
                        .rowType(RowType.of(DataTypes.INT()))
                        .location(tempDir.toUri().toString())
                        .options(Collections.emptyMap())
                        .build();
        assertThat(viewOf(lanceTable).originOnly).isTrue();
    }

    @Test
    void testBranchesAndExternalPathsTakeTheirOwnView() throws Exception {
        ViewFileIO fileIO = new ViewFileIO(false, TableFileIOView.NONE);
        String external = tempDir.resolve("external").toUri().toString();
        try (FileSystemCatalog catalog =
                new FileSystemCatalog(fileIO, new Path(tempDir.toUri().toString()))) {
            catalog.createDatabase("db", false);
            Identifier identifier = Identifier.create("db", "t");
            catalog.createTable(
                    identifier,
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .option(CoreOptions.DATA_FILE_PREFIX.key(), "main-")
                            .option(CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), external)
                            .build(),
                    false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            // only the table's own location: its external paths read from origin
            assertThat(viewOf(table).view.roots()).containsExactly(root(table.location()));

            table.createBranch("dev");
            FileStoreTable dev = table.switchToBranch("dev");
            assertThat(viewOf(dev).view.history())
                    .isEqualTo(
                            new TableFileIOViews.SchemaFilePrefixHistory(table.location(), "dev"));
            new FileSystemSchemaManager(fileIO, table.location(), "dev")
                    .commitChanges(
                            SchemaChange.setOption(CoreOptions.DATA_FILE_PREFIX.key(), "branch-"));
            FileStoreTable reloaded =
                    (FileStoreTable) catalog.getTable(new Identifier("db", "t", "dev"));
            assertThat(viewOf(reloaded).view.filePrefixes()).containsExactly("branch-");
            assertThat(viewOf(table.switchToBranch("dev")).view.filePrefixes()).contains("branch-");

            // the history of a branch lists its own schemas, from storage
            assertThat(
                            new TableFileIOViews.SchemaFilePrefixHistory(table.location(), "dev")
                                    .filePrefixes(LocalFileIO.create()))
                    .containsExactlyInAnyOrder("main-", "branch-");
        }
    }

    @Test
    void testFileIOsWithoutViewsStayTheSameInstance() throws Exception {
        // a decorator that forwards every FileIO method, default ones included
        LocalFileIO local = LocalFileIO.create();
        FileIO forwarding =
                (FileIO)
                        Proxy.newProxyInstance(
                                FileIO.class.getClassLoader(),
                                new Class<?>[] {FileIO.class},
                                (proxy, method, args) -> {
                                    try {
                                        return method.invoke(local, args);
                                    } catch (InvocationTargetException e) {
                                        throw e.getCause();
                                    }
                                });
        FormatTable formatTable =
                FormatTable.builder()
                        .fileIO(forwarding)
                        .identifier(Identifier.create("db", "format"))
                        .rowType(RowType.of(DataTypes.INT()))
                        .partitionKeys(Collections.emptyList())
                        .location(tempDir.resolve("format").toUri().toString())
                        .format(FormatTable.Format.PARQUET)
                        .options(Collections.emptyMap())
                        .build();
        assertThat(formatTable.fileIO()).isSameAs(forwarding);
        ObjectTable objectTable =
                ObjectTable.builder()
                        .fileIO(forwarding)
                        .identifier(Identifier.create("db", "objects"))
                        .location(tempDir.resolve("objects").toUri().toString())
                        .options(Collections.emptyMap())
                        .build();
        assertThat(objectTable.fileIO()).isSameAs(forwarding);
        try (FileSystemCatalog catalog =
                new FileSystemCatalog(forwarding, new Path(tempDir.toUri().toString()))) {
            catalog.createDatabase("db", false);
            Identifier identifier = Identifier.create("db", "t");
            catalog.createTable(
                    identifier, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
            assertThat(catalog.getTable(identifier).fileIO()).isSameAs(forwarding);
        }
    }

    private static ViewFileIO viewOf(Table table) {
        return (ViewFileIO) table.fileIO();
    }

    private static String root(Path path) {
        return TableFileIOView.of(Collections.singletonList(path), Collections.emptyList(), null)
                .roots()
                .get(0);
    }

    /** A FileIO that records the views tables take. */
    private static class ViewFileIO extends LocalFileIO implements IoCacheViewableFileIO {

        private static final long serialVersionUID = 1L;

        private final boolean originOnly;
        private final TableFileIOView view;

        private ViewFileIO(boolean originOnly, TableFileIOView view) {
            this.originOnly = originOnly;
            this.view = view;
        }

        @Override
        public FileIO originOnly() {
            return originOnly ? this : new ViewFileIO(true, view);
        }

        @Override
        public FileIO withTableView(TableFileIOView tableView) {
            TableFileIOView merged = view.merge(tableView);
            return originOnly || merged.equals(view) ? this : new ViewFileIO(false, merged);
        }
    }
}
