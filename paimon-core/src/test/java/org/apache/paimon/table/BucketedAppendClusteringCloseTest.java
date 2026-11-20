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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for releasing clustering spill files when the output writer fails to close. */
class BucketedAppendClusteringCloseTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testClusteringReleasesSpills(boolean asyncWrite, boolean failClose) throws Exception {
        java.nio.file.Path spillDir = Files.createDirectory(tempDir.resolve("spill"));
        CloseFailingFileIO fileIO = new CloseFailingFileIO(spillDir);
        Identifier identifier = Identifier.create("default", "test");
        try (Catalog catalog =
                        new FileSystemCatalog(
                                fileIO, new Path(tempDir.resolve("warehouse").toString()));
                IOManager io = IOManager.create(spillDir.toString())) {
            catalog.createDatabase("default", false);
            catalog.createTable(
                    identifier,
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("payload", DataTypes.STRING())
                            .option(CoreOptions.BUCKET.key(), "1")
                            .option(CoreOptions.BUCKET_KEY.key(), "id")
                            .option(CoreOptions.BUCKET_APPEND_ORDERED.key(), "false")
                            .option(CoreOptions.CLUSTERING_INCREMENTAL.key(), "true")
                            .option(CoreOptions.CLUSTERING_COLUMNS.key(), "id")
                            .option(CoreOptions.FILE_FORMAT.key(), "avro")
                            .option(CoreOptions.WRITE_BUFFER_SIZE.key(), "16 kb")
                            .option(CoreOptions.PAGE_SIZE.key(), "1 kb")
                            .option(CoreOptions.ASYNC_FILE_WRITE.key(), String.valueOf(asyncWrite))
                            .build(),
                    false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            String payload = String.join("", Collections.nCopies(512, "x"));
            List<Integer> expected = IntStream.range(0, 600).boxed().collect(Collectors.toList());
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                write.withIOManager(io);
                for (int i = 599; i >= 0; i--) {
                    write.write(GenericRow.of(i, BinaryString.fromString(payload + i)));
                }
                commit.commit(write.prepareCommit());
            }
            assertThat(spillFiles(spillDir)).isZero();
            List<String> originalFiles =
                    table.store().newScan().plan().files().stream()
                            .map(entry -> entry.file().fileName())
                            .collect(Collectors.toList());

            fileIO.observeClose = true;
            fileIO.failClose = failClose;
            builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                write.withIOManager(io);
                write.compact(BinaryRow.EMPTY_ROW, 0, true);
                if (failClose) {
                    assertThatThrownBy(write::prepareCommit)
                            .hasRootCauseMessage("Injected clustering output close failure");
                    assertThat(fileIO.failures).isEqualTo(1);
                    assertThat(table.store().newScan().plan().files())
                            .extracting(entry -> entry.file().fileName())
                            .containsExactlyElementsOf(originalFiles);
                } else {
                    commit.commit(write.prepareCommit());
                }
                assertThat(fileIO.spillsAtClose).isPositive();
                // Check before closing either the table write or its caller-owned IOManager.
                assertThat(spillFiles(spillDir)).isZero();
                assertThat(Files.exists(io.createChannel().getPathFile().toPath().getParent()))
                        .isTrue();
                assertThat(readIds(table)).containsExactlyInAnyOrderElementsOf(expected);
                if (!failClose) {
                    assertThat(readIds(table)).containsExactlyElementsOf(expected);
                }
            }
        }
    }

    private static List<Integer> readIds(FileStoreTable table) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder()
                        .newRead()
                        .createReader(table.newReadBuilder().newScan().plan())) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        return ids;
    }

    private static long spillFiles(java.nio.file.Path path) throws IOException {
        try (Stream<java.nio.file.Path> files = Files.walk(path)) {
            return files.filter(Files::isRegularFile).count();
        }
    }

    private static class CloseFailingFileIO extends LocalFileIO {

        private final java.nio.file.Path spillDir;
        private boolean observeClose;
        private boolean failClose;
        private int failures;
        private long spillsAtClose;

        private CloseFailingFileIO(java.nio.file.Path spillDir) {
            this.spillDir = spillDir;
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            return new PositionOutputStreamWrapper(super.newOutputStream(path, overwrite)) {
                @Override
                public void close() throws IOException {
                    super.close();
                    if (observeClose && path.getName().startsWith("data-")) {
                        observeClose = false;
                        spillsAtClose = spillFiles(spillDir);
                        if (failClose) {
                            failures++;
                            throw new IOException("Injected clustering output close failure");
                        }
                    }
                }
            };
        }
    }
}
