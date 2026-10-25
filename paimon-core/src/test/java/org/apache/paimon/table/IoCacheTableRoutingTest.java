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
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.TableFileIOView;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.UriReader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads through an io-cache target that keeps the first copy it served while origin moves on, as
 * the review of apache/paimon-rust#1014 does, through the catalog API and real writers.
 */
class IoCacheTableRoutingTest {

    private static final byte[] OLD = "OLD".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NEW = "NEW".getBytes(StandardCharsets.UTF_8);

    @TempDir java.nio.file.Path tempDir;

    private Target target;
    private FileSystemCatalog catalog;

    @BeforeEach
    void before() throws Exception {
        target = new Target();
        catalog =
                new FileSystemCatalog(
                        new TargetRetainingFileIO(target, false, TableFileIOView.NONE),
                        new Path(tempDir.resolve("warehouse").toUri().toString()));
        catalog.createDatabase("db", true);
    }

    @ParameterizedTest(name = "primary key {0}")
    @ValueSource(booleans = {true, false})
    void testDescriptorFieldsOutsideTheTableAreReadFromTheOrigin(boolean primaryKey)
            throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.BLOB())
                        .option(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "payload");
        if (primaryKey) {
            schema.primaryKey("id").option(CoreOptions.BUCKET.key(), "1");
        } else {
            schema.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                    .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                    .option(CoreOptions.BLOB_FIELD.key(), "payload");
        }
        FileStoreTable table = createTable("descriptors", schema.build());
        Path source = replacedSource();

        write(table, GenericRow.of(1, descriptorOf(LocalFileIO.create(), source)));

        List<InternalRow> rows = read(table);
        assertThat(rows.get(0).getBlob(1).toData()).isEqualTo(NEW);
        assertThat(target.reads).doesNotContain(source.getName());
        // the data file the table wrote still uses the target
        assertThat(target.reads).anyMatch(name -> name.startsWith("data-"));
    }

    @Test
    void testBlobSourcesOutsideTheTableAreCopiedFromTheOrigin() throws Exception {
        FileStoreTable table =
                createTable(
                        "managed",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("payload", DataTypes.BLOB())
                                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                                .option(CoreOptions.BLOB_FIELD.key(), "payload")
                                .build());
        Path source = replacedSource();

        // the writer reads the source through the table's FileIO and copies it into a .blob file
        write(table, GenericRow.of(1, descriptorOf(table.fileIO(), source)));

        assertThat(target.reads).doesNotContain(source.getName());
        assertThat(read(table).get(0).getBlob(1).toData()).isEqualTo(NEW);
    }

    @Test
    void testDataFilesUnderExternalPathsAreReadFromTheOrigin() throws Exception {
        String external = tempDir.resolve("external").toUri().toString();
        FileStoreTable table =
                createTable(
                        "external",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .option(CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), external)
                                .option(
                                        CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                                        "round-robin")
                                .build());
        write(table, GenericRow.of(1));
        List<java.nio.file.Path> externalFiles = new ArrayList<>();
        try (Stream<java.nio.file.Path> files = Files.walk(tempDir.resolve("external"))) {
            files.filter(Files::isRegularFile).forEach(externalFiles::add);
        }
        assertThat(externalFiles)
                .anyMatch(file -> file.getFileName().toString().startsWith("data-"));

        target.reads.clear();
        assertThat(read(table)).hasSize(1);
        for (java.nio.file.Path file : externalFiles) {
            assertThat(target.reads).doesNotContain(file.getFileName().toString());
        }
    }

    @Test
    void testFilesOfOlderSchemaPrefixesUseTheTargetAfterAReload() throws Exception {
        FileStoreTable table =
                createTable(
                        "altered",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .option(CoreOptions.DATA_FILE_PREFIX.key(), "older-")
                                .build());
        write(table, GenericRow.of(1));
        new FileSystemSchemaManager(table.fileIO(), table.location())
                .commitChanges(
                        SchemaChange.setOption(CoreOptions.DATA_FILE_PREFIX.key(), "newer-"));
        write(table(table.name()), GenericRow.of(2));

        target.reads.clear();
        // a fresh catalog knows only the newest schema until a file needs the older ones
        before();
        assertThat(read(table(table.name()))).hasSize(2);
        assertThat(target.reads).anyMatch(name -> name.startsWith("older-"));
        assertThat(target.reads).anyMatch(name -> name.startsWith("newer-"));
        assertThat(target.historyLoads).hasSize(1);
    }

    @Test
    void testBranchFilesUseTheTargetWithTheBranchPrefix() throws Exception {
        FileStoreTable table =
                createTable(
                        "branched",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .option(CoreOptions.DATA_FILE_PREFIX.key(), "main-")
                                .build());
        table.createBranch("dev");
        new FileSystemSchemaManager(table.fileIO(), table.location(), "dev")
                .commitChanges(
                        SchemaChange.setOption(CoreOptions.DATA_FILE_PREFIX.key(), "branch-"));
        Identifier dev = new Identifier("db", table.name(), "dev");
        write((FileStoreTable) catalog.getTable(dev), GenericRow.of(1));

        for (FileStoreTable branch :
                new FileStoreTable[] {
                    (FileStoreTable) catalog.getTable(dev),
                    table(table.name()).switchToBranch("dev")
                }) {
            target.reads.clear();
            assertThat(read(branch)).hasSize(1);
            assertThat(target.reads).anyMatch(name -> name.startsWith("branch-"));
        }
    }

    private FileStoreTable createTable(String name, Schema schema) throws Exception {
        catalog.createTable(Identifier.create("db", name), schema, false);
        return table(name);
    }

    private FileStoreTable table(String name) throws Exception {
        return (FileStoreTable) catalog.getTable(Identifier.create("db", name));
    }

    // Named like a data file and outside the table; the target keeps OLD, origin has NEW.
    private Path replacedSource() throws IOException {
        Path source =
                new Path(
                        tempDir.resolve("import")
                                .resolve("data-" + UUID.randomUUID() + "-1.png")
                                .toUri()
                                .toString());
        LocalFileIO.create().writeFile(source, new String(NEW, StandardCharsets.UTF_8), false);
        target.retained.put(source.toString(), OLD);
        return source;
    }

    private static Blob descriptorOf(FileIO fileIO, Path source) {
        return Blob.fromDescriptor(
                UriReader.fromFile(fileIO), new BlobDescriptor(source.toString(), 0, NEW.length));
    }

    private static void write(FileStoreTable table, InternalRow row) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(row);
            commit.commit(write.prepareCommit());
        }
    }

    private static List<InternalRow> read(FileStoreTable table) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        List<InternalRow> rows = new ArrayList<>();
        builder.newRead().createReader(builder.newScan().plan()).forEachRemaining(rows::add);
        return rows;
    }

    /** What the io-cache target serves and keeps, shared by every view. */
    private static class Target implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Map<String, byte[]> retained = new ConcurrentHashMap<>();
        private final List<String> reads = new CopyOnWriteArrayList<>();
        private final List<String> historyLoads = new CopyOnWriteArrayList<>();
    }

    /**
     * Local files whose reads of Paimon data files go to a target that keeps its first copy, routed
     * with the table view the way JindoFileIO routes them.
     */
    private static class TargetRetainingFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        private static final String SUFFIX =
                "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                        + "(-[0-9]+\\..+|\\.managed\\.blob)";
        private static final Pattern DATA_FILE_SUFFIX = Pattern.compile(SUFFIX);
        private static final Pattern ANY_PREFIX_DATA_FILE = Pattern.compile(".*" + SUFFIX);

        private final Target target;
        private final boolean originOnly;
        private final TableFileIOView view;
        private transient volatile List<String> olderPrefixes;

        private TargetRetainingFileIO(Target target, boolean originOnly, TableFileIOView view) {
            this.target = target;
            this.originOnly = originOnly;
            this.view = view;
        }

        @Override
        public FileIO originOnly() {
            return originOnly ? this : new TargetRetainingFileIO(target, true, view);
        }

        @Override
        public FileIO withTableView(TableFileIOView tableView) {
            TableFileIOView merged = view.merge(tableView);
            return originOnly || merged.equals(view)
                    ? this
                    : new TargetRetainingFileIO(target, false, merged);
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            if (!routes(path)) {
                return super.newInputStream(path);
            }
            target.reads.add(path.getName());
            byte[] bytes = target.retained.get(path.toString());
            if (bytes == null) {
                bytes = readAll(path);
                target.retained.put(path.toString(), bytes);
            }
            return new ByteArraySeekableStream(bytes);
        }

        private byte[] readAll(Path path) throws IOException {
            try (SeekableInputStream in = super.newInputStream(path)) {
                byte[] bytes = new byte[(int) getFileStatus(path).getLen()];
                int read = 0;
                while (read < bytes.length) {
                    int n = in.read(bytes, read, bytes.length - read);
                    if (n < 0) {
                        break;
                    }
                    read += n;
                }
                return bytes;
            }
        }

        private boolean routes(Path path) throws IOException {
            if (originOnly
                    || (!view.roots().isEmpty() && !TableFileIOView.isUnder(view.roots(), path))) {
                return false;
            }
            List<String> prefixes = new ArrayList<>(view.filePrefixes());
            prefixes.add("data-");
            prefixes.add("changelog-");
            if (isDataFile(path.getName(), prefixes)) {
                return true;
            }
            if (view.history() == null || !ANY_PREFIX_DATA_FILE.matcher(path.getName()).matches()) {
                return false;
            }
            if (olderPrefixes == null) {
                target.historyLoads.add(path.getName());
                olderPrefixes = view.history().filePrefixes(originOnly());
            }
            return isDataFile(path.getName(), olderPrefixes);
        }

        private static boolean isDataFile(String name, List<String> prefixes) {
            for (String prefix : prefixes) {
                if (name.startsWith(prefix)
                        && DATA_FILE_SUFFIX.matcher(name.substring(prefix.length())).matches()) {
                    return true;
                }
            }
            return false;
        }
    }
}
