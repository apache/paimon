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

package org.apache.paimon.table.format;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.TARGET_FILE_ROW_NUM;
import static org.apache.paimon.CoreOptions.TARGET_FILE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for writing a {@link FormatTable}. */
class FormatTableWriteTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testRollsByTargetRowNumber() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        FormatTable table = formatTable(tablePath, fileIO, 2);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        List<CommitMessage> messages;
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            for (int i = 0; i < 5; i++) {
                write.write(GenericRow.of(i));
            }
            messages = write.prepareCommit();
        }

        assertThat(messages).hasSize(3);
        List<Path> dataFiles =
                messages.stream()
                        .map(
                                message ->
                                        ((TwoPhaseCommitMessage) message)
                                                .getCommitter()
                                                .targetPath())
                        .collect(Collectors.toList());
        try (BatchTableCommit commit = writeBuilder.newCommit()) {
            commit.commit(messages);
        }

        List<Long> rowCounts =
                dataFiles.stream()
                        .map(
                                path -> {
                                    try (BufferedReader reader =
                                            new BufferedReader(
                                                    new InputStreamReader(
                                                            fileIO.newInputStream(path),
                                                            StandardCharsets.UTF_8))) {
                                        return reader.lines().count();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .collect(Collectors.toList());
        assertThat(rowCounts).containsExactlyInAnyOrder(2L, 2L, 1L);
    }

    @Test
    void testRejectsNonPositiveTargetRowNumber() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        FormatTable table = formatTable(tablePath, LocalFileIO.create(), 0);

        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite()) {
            assertThatThrownBy(() -> write.write(GenericRow.of(1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("targetFileRowNum must be positive, but is 0");
        }
    }

    @Test
    void testAbortAfterRollingDeletesTemporaryFiles() throws Exception {
        FormatTable table = formatTable(new Path(tempDir.toUri()), LocalFileIO.create(), 1);

        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite()) {
            write.write(GenericRow.of(1));
        }

        try (Stream<java.nio.file.Path> files = Files.walk(tempDir)) {
            assertThat(files.filter(Files::isRegularFile).count()).isZero();
        }
    }

    @Test
    void testPrepareCommitFailureDiscardsPreparedFiles() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Options options = new Options();
        options.set(PATH, new Path(tempDir.toUri()).toString());
        options.set(FILE_FORMAT, "csv");
        FormatTableFileWriter fileWriter =
                new FormatTableFileWriter(
                        fileIO,
                        RowType.of(DataTypes.INT()),
                        new org.apache.paimon.CoreOptions(options),
                        RowType.of(DataTypes.INT()));
        FormatTableRecordWriter recordWriter = mock(FormatTableRecordWriter.class);
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        java.util.concurrent.atomic.AtomicInteger closeCount =
                new java.util.concurrent.atomic.AtomicInteger();
        when(recordWriter.closeAndGetCommitters())
                .thenAnswer(
                        ignored -> {
                            if (closeCount.getAndIncrement() == 0) {
                                return Collections.singletonList(committer);
                            }
                            throw new IOException("expected close failure");
                        });
        fileWriter.writers.put(BinaryRow.singleColumn(1), recordWriter);
        fileWriter.writers.put(BinaryRow.singleColumn(2), recordWriter);

        assertThatThrownBy(fileWriter::prepareCommit)
                .isInstanceOf(IOException.class)
                .hasMessage("expected close failure");
        verify(committer).discard(fileIO);
        verify(recordWriter, atLeastOnce()).close();
    }

    private static FormatTable formatTable(
            Path tablePath, LocalFileIO fileIO, long targetFileRowNum) {
        Map<String, String> options = new HashMap<>();
        options.put(PATH.key(), tablePath.toString());
        options.put(FILE_FORMAT.key(), "csv");
        options.put(TARGET_FILE_SIZE.key(), "1 gb");
        options.put(TARGET_FILE_ROW_NUM.key(), Long.toString(targetFileRowNum));
        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(Identifier.create("test_db", "test_table"))
                .rowType(RowType.of(DataTypes.INT()))
                .partitionKeys(Collections.emptyList())
                .location(tablePath.toString())
                .format(FormatTable.Format.CSV)
                .options(options)
                .build();
    }
}
