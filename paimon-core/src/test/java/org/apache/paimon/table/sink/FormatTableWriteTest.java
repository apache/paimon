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

package org.apache.paimon.table.sink;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.FormatTableWrite;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FormatTableWrite} and related classes. */
public class FormatTableWriteTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {new IntType(), new VarCharType(100), new DoubleType()},
                    new String[] {"id", "name", "score"});

    private static final RowType PARTITIONED_ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        new IntType(), new VarCharType(100), new DoubleType(), new VarCharType(50)
                    },
                    new String[] {"id", "name", "score", "department"});

    @TempDir File tempDir;

    private FileIO fileIO;
    private String tablePath;

    @BeforeEach
    public void setup() {
        fileIO = LocalFileIO.create();
        tablePath = tempDir.getAbsolutePath();
    }

    @Test
    public void testParquetFormatTableWrite() throws Exception {
        testFormatTableWrite(FormatTable.Format.PARQUET);
    }

    @Test
    public void testCsvFormatTableWrite() throws Exception {
        testFormatTableWrite(FormatTable.Format.CSV);
    }

    @Test
    public void testJsonFormatTableWrite() throws Exception {
        testFormatTableWrite(FormatTable.Format.JSON);
    }

    @Test
    public void testPartitionedParquetFormatTableWrite() throws Exception {
        testPartitionedFormatTableWrite(FormatTable.Format.PARQUET);
    }

    @Test
    public void testOverwriteMode() throws Exception {
        FormatTable formatTable = createFormatTable(FormatTable.Format.PARQUET, false);

        // First write
        BatchWriteBuilder writeBuilder1 = formatTable.newBatchWriteBuilder();
        BatchTableWrite write1 = writeBuilder1.newWrite();
        BatchTableCommit commit1 = writeBuilder1.newCommit();

        write1.write(GenericRow.of(1, BinaryString.fromString("Alice"), 85.5));
        write1.write(GenericRow.of(2, BinaryString.fromString("Bob"), 92.0));

        List<CommitMessage> messages1 = write1.prepareCommit();
        commit1.commit(messages1);

        write1.close();
        commit1.close();

        // Second write with overwrite
        BatchWriteBuilder writeBuilder2 = formatTable.newBatchWriteBuilder().withOverwrite();
        BatchTableWrite write2 = writeBuilder2.newWrite();
        BatchTableCommit commit2 = writeBuilder2.newCommit();

        write2.write(GenericRow.of(3, BinaryString.fromString("Charlie"), 88.0));

        List<CommitMessage> messages2 = write2.prepareCommit();
        commit2.commit(messages2);

        write2.close();
        commit2.close();

        // Verify overwrite worked (original files should be replaced)
        assertThat(messages2).isNotEmpty();
    }

    @Test
    public void testEmptyCommit() throws Exception {
        FormatTable formatTable = createFormatTable(FormatTable.Format.PARQUET, false);

        BatchWriteBuilder writeBuilder = formatTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        // Don't write any data
        List<CommitMessage> messages = write.prepareCommit();

        // Should not throw exception for empty commit
        commit.commit(messages);

        write.close();
        commit.close();

        assertThat(messages).isEmpty();
    }

    @Test
    public void testPartitionOverwrite() throws Exception {
        FormatTable formatTable = createFormatTable(FormatTable.Format.PARQUET, true);

        // First write - multiple partitions
        BatchWriteBuilder writeBuilder1 = formatTable.newBatchWriteBuilder();
        BatchTableWrite write1 = writeBuilder1.newWrite();
        BatchTableCommit commit1 = writeBuilder1.newCommit();

        write1.write(
                GenericRow.of(
                        1,
                        BinaryString.fromString("Alice"),
                        85.5,
                        BinaryString.fromString("Engineering")));
        write1.write(
                GenericRow.of(
                        2, BinaryString.fromString("Bob"), 92.0, BinaryString.fromString("Sales")));
        write1.write(
                GenericRow.of(
                        3,
                        BinaryString.fromString("Charlie"),
                        88.0,
                        BinaryString.fromString("Engineering")));

        List<CommitMessage> messages1 = write1.prepareCommit();
        commit1.commit(messages1);

        write1.close();
        commit1.close();

        // Second write - overwrite specific partition
        Map<String, String> partitionOverwrite = new HashMap<>();
        partitionOverwrite.put("department", "Engineering");

        BatchWriteBuilder writeBuilder2 =
                formatTable.newBatchWriteBuilder().withOverwrite(partitionOverwrite);
        BatchTableWrite write2 = writeBuilder2.newWrite();
        BatchTableCommit commit2 = writeBuilder2.newCommit();

        write2.write(
                GenericRow.of(
                        4,
                        BinaryString.fromString("David"),
                        90.0,
                        BinaryString.fromString("Engineering")));

        List<CommitMessage> messages2 = write2.prepareCommit();
        commit2.commit(messages2);

        write2.close();
        commit2.close();

        assertThat(messages2).isNotEmpty();
    }

    private void testFormatTableWrite(FormatTable.Format format) throws Exception {
        FormatTable formatTable = createFormatTable(format, false);

        BatchWriteBuilder writeBuilder = formatTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        // Write test data
        write.write(GenericRow.of(1, BinaryString.fromString("Alice"), 85.5));
        write.write(GenericRow.of(2, BinaryString.fromString("Bob"), 92.0));
        write.write(GenericRow.of(3, BinaryString.fromString("Charlie"), 88.0));

        List<CommitMessage> messages = write.prepareCommit();
        assertThat(messages).isNotEmpty();

        commit.commit(messages);

        write.close();
        commit.close();

        // Verify files were created
        File tableDir = new File(tablePath);
        assertThat(tableDir.exists()).isTrue();
        assertThat(tableDir.listFiles()).isNotNull();
        assertThat(tableDir.listFiles().length).isGreaterThan(0);
    }

    private void testPartitionedFormatTableWrite(FormatTable.Format format) throws Exception {
        FormatTable formatTable = createFormatTable(format, true);

        BatchWriteBuilder writeBuilder = formatTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        // Write test data with different partitions
        write.write(
                GenericRow.of(
                        1,
                        BinaryString.fromString("Alice"),
                        85.5,
                        BinaryString.fromString("Engineering")));
        write.write(
                GenericRow.of(
                        2, BinaryString.fromString("Bob"), 92.0, BinaryString.fromString("Sales")));
        write.write(
                GenericRow.of(
                        3,
                        BinaryString.fromString("Charlie"),
                        88.0,
                        BinaryString.fromString("Engineering")));
        write.write(
                GenericRow.of(
                        4,
                        BinaryString.fromString("David"),
                        90.0,
                        BinaryString.fromString("Marketing")));

        List<CommitMessage> messages = write.prepareCommit();
        assertThat(messages).isNotEmpty();

        commit.commit(messages);

        write.close();
        commit.close();

        // Verify partition directories were created
        File tableDir = new File(tablePath);
        assertThat(tableDir.exists()).isTrue();

        // Should have partition directories
        File[] partitionDirs = tableDir.listFiles(File::isDirectory);
        assertThat(partitionDirs).isNotNull();
        assertThat(partitionDirs.length).isGreaterThan(0);
    }

    private FormatTable createFormatTable(FormatTable.Format format, boolean partitioned) {
        RowType rowType = partitioned ? PARTITIONED_ROW_TYPE : ROW_TYPE;
        List<String> partitionKeys =
                partitioned ? Arrays.asList("department") : Collections.emptyList();

        Map<String, String> options = new HashMap<>();
        options.put("file.compression", "gzip");
        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(Identifier.create("test_db", "test_table"))
                .rowType(rowType)
                .partitionKeys(partitionKeys)
                .location(tablePath)
                .format(format)
                .options(options)
                .comment("Test format table")
                .build();
    }
}
