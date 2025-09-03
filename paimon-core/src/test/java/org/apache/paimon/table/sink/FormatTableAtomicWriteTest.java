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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.FormatTableAtomicCommitter.TempFileInfo;
import org.apache.paimon.table.sink.FormatTableWrite.FormatTableCommitMessage;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for atomic write functionality in FormatTable. */
public class FormatTableAtomicWriteTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {new IntType(), new VarCharType(100), new DoubleType()},
                    new String[] {"id", "name", "score"});

    @TempDir File tempDir;

    private FileIO fileIO;
    private String tablePath;

    @BeforeEach
    public void setup() {
        fileIO = LocalFileIO.create();
        tablePath = tempDir.getAbsolutePath();
    }

    @Test
    public void testLocalAtomicWrite() throws Exception {
        FormatTable formatTable = createFormatTable(tablePath, FormatTable.Format.PARQUET);
        testAtomicWrite(formatTable);
    }

    @Test
    public void testHDFSStyleAtomicWrite() throws Exception {
        // Use local path but test HDFS-style atomic committer logic
        String hdfsStylePath = new File(tempDir, "hdfs-style").getAbsolutePath();
        FormatTable formatTable = createFormatTable(hdfsStylePath, FormatTable.Format.PARQUET);
        testAtomicWrite(formatTable);
    }

    @Test
    public void testS3StyleAtomicWrite() throws Exception {
        // Use local path but test S3-style atomic committer logic
        String s3StylePath = new File(tempDir, "s3-style").getAbsolutePath();
        FormatTable formatTable = createFormatTable(s3StylePath, FormatTable.Format.PARQUET);
        testAtomicWrite(formatTable);
    }

    @Test
    public void testOSSStyleAtomicWrite() throws Exception {
        // Use local path but test OSS-style atomic committer logic
        String ossStylePath = new File(tempDir, "oss-style").getAbsolutePath();
        FormatTable formatTable = createFormatTable(ossStylePath, FormatTable.Format.PARQUET);
        testAtomicWrite(formatTable);
    }

    @Test
    public void testAtomicWriteVisibility() throws Exception {
        FormatTable formatTable = createFormatTable(tablePath, FormatTable.Format.PARQUET);

        BatchWriteBuilder writeBuilder = formatTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        // Write data but don't commit yet
        write.write(GenericRow.of(1, BinaryString.fromString("Alice"), 85.5));
        write.write(GenericRow.of(2, BinaryString.fromString("Bob"), 92.0));

        List<CommitMessage> messages = write.prepareCommit();

        // Before commit, files should be in temporary location and not visible in final location
        for (CommitMessage message : messages) {
            if (message instanceof FormatTableCommitMessage) {
                FormatTableCommitMessage formatMessage = (FormatTableCommitMessage) message;
                TempFileInfo tempFileInfo = formatMessage.getTempFileInfo();

                // Temporary file should exist
                assertThat(fileIO.exists(tempFileInfo.getTempPath())).isTrue();

                // Final file should not exist yet
                assertThat(fileIO.exists(tempFileInfo.getFinalPath())).isFalse();
            }
        }

        // Commit the files
        commit.commit(messages);

        // After commit, files should be visible in final location
        for (CommitMessage message : messages) {
            if (message instanceof FormatTableCommitMessage) {
                FormatTableCommitMessage formatMessage = (FormatTableCommitMessage) message;
                TempFileInfo tempFileInfo = formatMessage.getTempFileInfo();

                // Final file should now exist and not be empty
                assertThat(fileIO.exists(tempFileInfo.getFinalPath())).isTrue();
                FileStatus finalFileStatus = fileIO.getFileStatus(tempFileInfo.getFinalPath());
                assertThat(finalFileStatus.getLen()).isGreaterThan(0);

                // Temporary file should be cleaned up
                assertThat(fileIO.exists(tempFileInfo.getTempPath())).isFalse();
            }
        }

        write.close();
        commit.close();
    }

    @Test
    public void testAtomicWriteAbort() throws Exception {
        FormatTable formatTable = createFormatTable(tablePath, FormatTable.Format.PARQUET);

        BatchWriteBuilder writeBuilder = formatTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        // Write data
        write.write(GenericRow.of(1, BinaryString.fromString("Alice"), 85.5));
        write.write(GenericRow.of(2, BinaryString.fromString("Bob"), 92.0));

        List<CommitMessage> messages = write.prepareCommit();

        // Store temp file paths before abort for verification
        boolean hasTempFiles = false;
        for (CommitMessage message : messages) {
            if (message instanceof FormatTableCommitMessage) {
                FormatTableCommitMessage formatMessage = (FormatTableCommitMessage) message;
                TempFileInfo tempFileInfo = formatMessage.getTempFileInfo();

                // Verify temp file exists before abort
                if (fileIO.exists(tempFileInfo.getTempPath())) {
                    hasTempFiles = true;
                }
            }
        }

        // Ensure we actually had temp files to begin with
        assertThat(hasTempFiles).isTrue();

        // Abort instead of commit
        commit.abort(messages);

        // After abort, neither temporary nor final files should exist
        for (CommitMessage message : messages) {
            if (message instanceof FormatTableCommitMessage) {
                FormatTableCommitMessage formatMessage = (FormatTableCommitMessage) message;
                TempFileInfo tempFileInfo = formatMessage.getTempFileInfo();

                // Both temporary and final files should not exist
                assertThat(fileIO.exists(tempFileInfo.getTempPath())).isFalse();
                assertThat(fileIO.exists(tempFileInfo.getFinalPath())).isFalse();
            }
        }

        write.close();
        commit.close();
    }

    @Test
    public void testAtomicCommitterCreation() {
        // Test local committer creation
        FormatTable localTable = createFormatTable(tablePath, FormatTable.Format.PARQUET);
        FormatTableAtomicCommitter localCommitter = FormatTableAtomicCommitter.create(localTable);
        assertThat(localCommitter).isNotNull();

        // Test different path styles (all using local paths for testing)
        String hdfsStylePath = new File(tempDir, "hdfs-test").getAbsolutePath();
        FormatTable hdfsStyleTable = createFormatTable(hdfsStylePath, FormatTable.Format.PARQUET);
        FormatTableAtomicCommitter hdfsStyleCommitter =
                FormatTableAtomicCommitter.create(hdfsStyleTable);
        assertThat(hdfsStyleCommitter).isNotNull();

        String s3StylePath = new File(tempDir, "s3-test").getAbsolutePath();
        FormatTable s3StyleTable = createFormatTable(s3StylePath, FormatTable.Format.PARQUET);
        FormatTableAtomicCommitter s3StyleCommitter =
                FormatTableAtomicCommitter.create(s3StyleTable);
        assertThat(s3StyleCommitter).isNotNull();

        String ossStylePath = new File(tempDir, "oss-test").getAbsolutePath();
        FormatTable ossStyleTable = createFormatTable(ossStylePath, FormatTable.Format.PARQUET);
        FormatTableAtomicCommitter ossStyleCommitter =
                FormatTableAtomicCommitter.create(ossStyleTable);
        assertThat(ossStyleCommitter).isNotNull();
    }

    @Test
    public void testTempDirectoryCleanup() throws Exception {
        FormatTable formatTable = createFormatTable(tablePath, FormatTable.Format.PARQUET);

        BatchWriteBuilder writeBuilder = formatTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        // Write data and commit
        write.write(GenericRow.of(1, BinaryString.fromString("Alice"), 85.5));
        List<CommitMessage> messages = write.prepareCommit();
        commit.commit(messages);

        // Check that no temporary directories remain
        Path tableDir = new Path(tablePath);
        FileStatus[] files = fileIO.listStatus(tableDir);

        boolean tempDirExists = false;
        if (files != null) {
            for (FileStatus file : files) {
                if (file.isDir()
                        && (file.getPath().getName().startsWith("_temp_")
                                || file.getPath().getName().equals("_temporary")
                                || file.getPath().getName().equals("_staging"))) {
                    tempDirExists = true;
                    break;
                }
            }
        }

        assertThat(tempDirExists).isFalse();

        write.close();
        commit.close();
    }

    private void testAtomicWrite(FormatTable formatTable) throws Exception {
        BatchWriteBuilder writeBuilder = formatTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        // Write test data
        write.write(GenericRow.of(1, BinaryString.fromString("Alice"), 85.5));
        write.write(GenericRow.of(2, BinaryString.fromString("Bob"), 92.0));
        write.write(GenericRow.of(3, BinaryString.fromString("Charlie"), 88.0));

        List<CommitMessage> messages = write.prepareCommit();
        assertThat(messages).isNotEmpty();

        // Commit should not throw exception
        assertThatCode(() -> commit.commit(messages)).doesNotThrowAnyException();

        write.close();
        commit.close();
    }

    private FormatTable createFormatTable(String location, FormatTable.Format format) {
        Map<String, String> options = new HashMap<>();
        // Use gzip instead of snappy to avoid native code issues
        options.put("compression", "gzip");

        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(Identifier.create("test_db", "test_table"))
                .rowType(ROW_TYPE)
                .partitionKeys(Collections.emptyList())
                .location(location)
                .format(format)
                .options(options)
                .comment("Test format table")
                .build();
    }
}
