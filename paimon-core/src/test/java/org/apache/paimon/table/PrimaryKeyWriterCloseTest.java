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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/** Tests for releasing primary-key write buffers when a table writer is closed. */
class PrimaryKeyWriterCloseTest extends TableTestBase {

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void testCloseReleasesWriteBuffer(boolean spillable, boolean commitBeforeClose)
            throws Exception {
        FileStoreTable table = createTable(spillable);
        HeapMemorySegmentPool pool = new HeapMemorySegmentPool(3 * 4096L, 4096);
        MemoryPoolFactory poolFactory = new MemoryPoolFactory(pool);
        Path spillDirectory = tempPath.resolve("spill");
        Files.createDirectories(spillDirectory);
        String value = String.join("", java.util.Collections.nCopies(900, "x"));
        int count = spillable ? 100 : 1;
        try (IOManager io = IOManager.create(spillDirectory.toString())) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                write.withMemoryPoolFactory(poolFactory);
                write.withIOManager(io);
                for (int i = 0; i < count; i++) {
                    write.write(GenericRow.of(i, BinaryString.fromString(value)));
                }
                assertThat(pool.freePages()).isLessThan(3);
                if (spillable) {
                    assertThat(filesIn(spillDirectory)).isNotEmpty();
                }
                if (commitBeforeClose) {
                    commit.commit(write.prepareCommit());
                }
            }
            // Check before closing the externally owned IO manager or releasing the shared pool.
            List<Path> remainingFiles = filesIn(spillDirectory);
            assertSoftly(
                    softly -> {
                        softly.assertThat(pool.freePages()).isEqualTo(3);
                        softly.assertThat(remainingFiles).isEmpty();
                    });
            if (commitBeforeClose) {
                assertThat(read(table))
                        .allSatisfy(row -> assertThat(row.getString(1).toString()).isEqualTo(value))
                        .extracting(row -> row.getInt(0))
                        .containsExactlyInAnyOrderElementsOf(
                                IntStream.range(0, count).boxed().collect(Collectors.toList()));
            } else {
                assertThat(table.snapshotManager().latestSnapshotId()).isNull();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCloseAllowsSharedPoolReuse(boolean spillable) throws Exception {
        FileStoreTable table = createTable(spillable);
        HeapMemorySegmentPool pool = new HeapMemorySegmentPool(3 * 4096L, 4096);
        MemoryPoolFactory poolFactory = new MemoryPoolFactory(pool);
        try (IOManager io = IOManager.create(tempPath.toString())) {
            for (int i = 0; i < 3; i++) {
                try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite()) {
                    write.withMemoryPoolFactory(poolFactory);
                    write.withIOManager(io);
                    write.write(GenericRow.of(i, BinaryString.fromString("value")));
                }
            }
            assertThat(pool.freePages()).isEqualTo(3);
            assertThat(table.snapshotManager().latestSnapshotId()).isNull();
        }
    }

    private FileStoreTable createTable(boolean spillable) throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("k")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.WRITE_BUFFER_SPILLABLE.key(), String.valueOf(spillable))
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .build(),
                false);
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private static List<Path> filesIn(Path directory) throws Exception {
        try (Stream<Path> files = Files.walk(directory)) {
            return files.filter(Files::isRegularFile).collect(Collectors.toList());
        }
    }
}
