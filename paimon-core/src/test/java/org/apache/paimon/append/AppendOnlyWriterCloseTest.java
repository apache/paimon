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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.ExternalBuffer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for releasing writer resources after a failed append compaction. */
class AppendOnlyWriterCloseTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFailedCompactionStillReleasesWriterResources(boolean buffered) throws Exception {
        checkFailedCompactionCleanup(buffered, false);
    }

    @Test
    void testCleanupFailureIsSuppressedAndDoesNotSkipBufferCleanup() throws Exception {
        checkFailedCompactionCleanup(true, true);
    }

    private void checkFailedCompactionCleanup(boolean buffered, boolean failManagerClose)
            throws Exception {
        IOException compactionFailure = new IOException("Injected compaction read failure");
        IOException cleanupFailure = new IOException("Injected manager close failure");
        CountDownLatch failCompaction = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IOManager ioManager = IOManager.create(tempDir.toString());
        RowType rowType = RowType.of(DataTypes.STRING());
        Options options = new Options();
        CoreOptions coreOptions = new CoreOptions(options);
        java.nio.file.Path dataDirectory = tempDir.resolve("data");
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        new Path(dataDirectory.toUri()),
                        CoreOptions.FILE_FORMAT_AVRO,
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        false,
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null);
        BucketedAppendCompactManager manager =
                new BucketedAppendCompactManager(
                        executor,
                        Collections.emptyList(),
                        null,
                        3,
                        1024 * 1024L,
                        1024 * 1024L,
                        false,
                        files -> {
                            if (!failCompaction.await(30, TimeUnit.SECONDS)) {
                                throw new IOException("Timed out waiting to fail compaction");
                            }
                            throw compactionFailure;
                        },
                        null) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        if (failManagerClose) {
                            throw cleanupFailure;
                        }
                    }
                };
        AppendOnlyWriter writer =
                new AppendOnlyWriter(
                        LocalFileIO.create(),
                        ioManager,
                        0,
                        FileFormat.fromIdentifier(CoreOptions.FILE_FORMAT_AVRO, options),
                        null,
                        1024 * 1024L,
                        1024 * 1024L,
                        1024 * 1024L,
                        Long.MAX_VALUE,
                        rowType,
                        null,
                        -1,
                        manager,
                        null,
                        false,
                        pathFactory,
                        null,
                        buffered,
                        buffered,
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        CompressOptions.defaultOptions(),
                        new StatsCollectorFactories(coreOptions),
                        MemorySize.MAX_VALUE,
                        new FileIndexOptions(),
                        false,
                        false,
                        false,
                        null,
                        null,
                        FileSource.APPEND,
                        false);
        writer.setMemoryPool(new HeapMemorySegmentPool(16 * 1024L, 1024));
        try {
            // Flush three real files to submit an asynchronous compaction.
            for (int i = 0; i < 3; i++) {
                writer.write(GenericRow.of(BinaryString.fromString("committed")));
                assertThat(writer.prepareCommit(false).newFilesIncrement().newFiles()).hasSize(1);
            }
            // Release the task after every prepare returns, so only close observes its failure.
            failCompaction.countDown();
            // A barrier on the same executor proves the failed task has finished. Cancelling
            // its completed future during close cannot discard its ExecutionException.
            executor.submit(() -> {}).get(10, TimeUnit.SECONDS);
            List<java.nio.file.Path> preparedFiles = filesIn(dataDirectory);
            assertThat(preparedFiles).hasSize(3);

            String value = String.join("", Collections.nCopies(990, "a"));
            for (int i = 0; i < 100; i++) {
                writer.write(GenericRow.of(BinaryString.fromString(value)));
            }
            List<File> spills = new ArrayList<>();
            if (buffered) {
                ((ExternalBuffer) writer.getWriteBuffer())
                        .getSpillChannels()
                        .forEach(channel -> spills.add(new File(channel.getChannel().getPath())));
                assertThat(spills).isNotEmpty().allMatch(File::exists);
            } else {
                assertThat(filesIn(dataDirectory)).hasSize(4);
            }

            Throwable thrown = catchThrowable(writer::close);
            assertThat(thrown).isInstanceOf(ExecutionException.class).hasCause(compactionFailure);
            // Check the resources before the externally owned IOManager is closed.
            assertThat(filesIn(dataDirectory)).containsExactlyInAnyOrderElementsOf(preparedFiles);
            if (buffered) {
                assertThat(spills).noneMatch(File::exists);
                assertThat(writer.getWriteBuffer()).isNull();
            }
            if (failManagerClose) {
                assertThat(thrown.getSuppressed()).containsExactly(cleanupFailure);
            }
        } finally {
            failCompaction.countDown();
            // A second close is only test-fixture cleanup, after all regression assertions.
            try {
                writer.close();
            } catch (IOException e) {
                if (e != cleanupFailure) {
                    throw e;
                }
            } finally {
                executor.shutdownNow();
                ioManager.close();
            }
        }
    }

    private List<java.nio.file.Path> filesIn(java.nio.file.Path directory) throws IOException {
        try (Stream<java.nio.file.Path> paths = Files.walk(directory)) {
            return paths.filter(Files::isRegularFile).collect(Collectors.toList());
        }
    }
}
