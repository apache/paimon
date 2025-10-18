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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RollingBlobFileWriter}. */
public class RollingBlobFileWriterTest {

    private static final RowType SCHEMA =
            RowType.builder()
                    .field("f0", DataTypes.INT())
                    .field("f1", DataTypes.STRING())
                    .field("f2", DataTypes.BLOB())
                    .build();

    private static final long TARGET_FILE_SIZE = 12 * 1024 * 1024L; // 12 MB
    private static final long SCHEMA_ID = 1L;
    private static final String COMPRESSION = "zstd";

    @TempDir java.nio.file.Path tempDir;

    private RollingBlobFileWriter writer;
    private DataFilePathFactory pathFactory;
    private LongCounter seqNumCounter;
    private byte[] testBlobData;

    @BeforeEach
    public void setUp() throws IOException {
        // Create test blob data
        testBlobData = new byte[1024 * 1024]; // 1 MB
        new Random(42).nextBytes(testBlobData);

        // Setup file system and path factory
        LocalFileIO fileIO = LocalFileIO.create();
        pathFactory =
                new DataFilePathFactory(
                        new Path(tempDir + "/bucket-0"),
                        "parquet",
                        "data",
                        "changelog",
                        false,
                        null,
                        null);
        seqNumCounter = new LongCounter();

        // Initialize the writer
        writer =
                new RollingBlobFileWriter(
                        fileIO,
                        SCHEMA_ID,
                        FileFormat.fromIdentifier("parquet", new Options()),
                        TARGET_FILE_SIZE,
                        TARGET_FILE_SIZE,
                        SCHEMA,
                        pathFactory,
                        seqNumCounter,
                        COMPRESSION,
                        new StatsCollectorFactories(new CoreOptions(new Options())),
                        new FileIndexOptions(),
                        FileSource.APPEND,
                        false, // asyncFileWrite
                        false // statsDenseStore
                        );
    }

    @Test
    public void testBasicWriting() throws IOException {
        // Write a single row with blob data
        InternalRow row =
                GenericRow.of(1, BinaryString.fromString("test"), new BlobData(testBlobData));
        writer.write(row);

        assertThat(writer.recordCount()).isEqualTo(1);
    }

    @Test
    public void testMultipleWrites() throws IOException {
        // Write multiple rows
        for (int i = 0; i < 36; i++) {
            InternalRow row =
                    GenericRow.of(
                            i, BinaryString.fromString("test" + i), new BlobData(testBlobData));
            writer.write(row);
        }

        writer.close();
        List<DataFileMeta> metasResult = writer.result();

        assertThat(metasResult.size()).isEqualTo(4);
        assertThat(metasResult.get(0).fileFormat()).isEqualTo("parquet");
        assertThat(metasResult.subList(1, 4)).allMatch(f -> f.fileFormat().equals("blob"));
        assertThat(writer.recordCount()).isEqualTo(36);

        assertThat(metasResult.get(0).rowCount())
                .isEqualTo(
                        metasResult.subList(1, 4).stream().mapToLong(DataFileMeta::rowCount).sum());
    }

    @Test
    public void testBundleWriting() throws IOException {
        // Create a bundle of records
        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(
                                1, BinaryString.fromString("test1"), new BlobData(testBlobData)),
                        GenericRow.of(
                                2, BinaryString.fromString("test2"), new BlobData(testBlobData)),
                        GenericRow.of(
                                3, BinaryString.fromString("test3"), new BlobData(testBlobData)));

        // Write bundle
        writer.writeBundle(new TestBundleRecords(rows));

        assertThat(writer.recordCount()).isEqualTo(3);
    }

    @Test
    public void testDoubleClose() throws IOException {
        // Write some data
        InternalRow row =
                GenericRow.of(1, BinaryString.fromString("test"), new BlobData(testBlobData));
        writer.write(row);

        // Close twice - should not throw exception
        writer.close();
        writer.close();

        // Should be able to get results
        List<DataFileMeta> results = writer.result();
        assertThat(results).isNotEmpty();
    }

    @Test
    public void testBlobTargetFileSize() throws IOException {
        // Set a specific blob target file size (different from regular target file size)
        long blobTargetFileSize = 500 * 1024 * 1024L; // 2 MB for blob files

        // Create a new writer with different blob target file size
        RollingBlobFileWriter blobSizeTestWriter =
                new RollingBlobFileWriter(
                        LocalFileIO.create(),
                        SCHEMA_ID,
                        FileFormat.fromIdentifier("parquet", new Options()),
                        128 * 1024 * 1024,
                        blobTargetFileSize, // Different blob target size
                        SCHEMA,
                        new DataFilePathFactory(
                                new Path(tempDir + "/blob-size-test"),
                                "parquet",
                                "data",
                                "changelog",
                                false,
                                null,
                                null),
                        new LongCounter(),
                        COMPRESSION,
                        new StatsCollectorFactories(new CoreOptions(new Options())),
                        new FileIndexOptions(),
                        FileSource.APPEND,
                        false, // asyncFileWrite
                        false // statsDenseStore
                        );

        // Create large blob data that will exceed the blob target file size
        byte[] largeBlobData = new byte[3 * 1024 * 1024]; // 3 MB blob data
        new Random(123).nextBytes(largeBlobData);

        // Write multiple rows with large blob data to trigger rolling
        for (int i = 0; i < 400; i++) {
            InternalRow row =
                    GenericRow.of(
                            i,
                            BinaryString.fromString("large-blob-test-" + i),
                            new BlobData(largeBlobData));
            blobSizeTestWriter.write(row);
        }

        blobSizeTestWriter.close();
        List<DataFileMeta> results = blobSizeTestWriter.result();

        // Verify that we have multiple files due to rolling
        assertThat(results.size()).isGreaterThan(1);

        // Check that blob files (format = "blob") meet the target size requirement
        List<DataFileMeta> blobFiles =
                results.stream()
                        .filter(file -> "blob".equals(file.fileFormat()))
                        .collect(java.util.stream.Collectors.toList());

        assertThat(blobFiles).isNotEmpty();

        // Verify that blob files are close to the target size (within reasonable tolerance)
        for (DataFileMeta blobFile : blobFiles.subList(0, blobFiles.size() - 1)) {
            long fileSize = blobFile.fileSize();
            assertThat(fileSize)
                    .as("Blob file size should be close to target size")
                    .isGreaterThanOrEqualTo(blobTargetFileSize)
                    .isLessThanOrEqualTo(blobTargetFileSize + largeBlobData.length);
        }

        // Verify total record count
        assertThat(blobSizeTestWriter.recordCount()).isEqualTo(400);
    }

    @Test
    public void testSchemaValidation() throws IOException {
        // Test that the writer correctly handles the schema with blob field
        InternalRow row =
                GenericRow.of(1, BinaryString.fromString("test"), new BlobData(testBlobData));
        writer.write(row);
        writer.close();

        List<DataFileMeta> results = writer.result();

        // Verify schema ID is set correctly
        results.forEach(file -> assertThat(file.schemaId()).isEqualTo(SCHEMA_ID));
    }

    /** Simple implementation of BundleRecords for testing. */
    private static class TestBundleRecords implements BundleRecords {
        private final List<InternalRow> rows;

        public TestBundleRecords(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Override
        public java.util.Iterator<InternalRow> iterator() {
            return rows.iterator();
        }

        @Override
        public long rowCount() {
            return rows.size();
        }
    }
}
