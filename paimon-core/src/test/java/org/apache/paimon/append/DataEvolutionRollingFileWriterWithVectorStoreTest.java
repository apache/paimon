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
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.StatsCollectorFactories;
import org.apache.paimon.utils.VectorStoreUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataEvolutionRollingFileWriter} with vector-store. */
public class DataEvolutionRollingFileWriterWithVectorStoreTest {

    private static final int VECTOR_DIM = 10;
    private static final RowType SCHEMA =
            RowType.builder()
                    .field("f0", DataTypes.INT())
                    .field("f1", DataTypes.STRING())
                    .field("f2", DataTypes.BLOB())
                    .field("f3", DataTypes.VECTOR(VECTOR_DIM, DataTypes.FLOAT()))
                    .field("f4", DataTypes.INT())
                    .build();

    private static final long TARGET_FILE_SIZE = 2 * 1024 * 1024L; // 2 MB
    private static final long VECTOR_STORE_TARGET_FILE_SIZE = 4 * 1024 * 1024L; // 4 MB
    private static final long SCHEMA_ID = 1L;
    private static final String COMPRESSION = "none";
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    @TempDir java.nio.file.Path tempDir;

    private DataEvolutionRollingFileWriter writer;
    private DataFilePathFactory pathFactory;
    private LongCounter seqNumCounter;

    @BeforeEach
    public void setUp() throws IOException {
        // Setup file system and path factory
        LocalFileIO fileIO = LocalFileIO.create();
        pathFactory =
                new DataFilePathFactory(
                        new Path(tempDir + "/bucket-0"),
                        "parquet",
                        "data-", // dataFilePrefix should include the hyphen to match expected
                        // format: data-{uuid}-{count}
                        "changelog",
                        false,
                        null,
                        null);
        seqNumCounter = new LongCounter();

        // Initialize the writer
        writer =
                new DataEvolutionRollingFileWriter(
                        fileIO,
                        SCHEMA_ID,
                        FileFormat.fromIdentifier("parquet", new Options()),
                        FileFormat.fromIdentifier("json", new Options()),
                        Arrays.asList("f3", "f4"),
                        TARGET_FILE_SIZE,
                        TARGET_FILE_SIZE,
                        VECTOR_STORE_TARGET_FILE_SIZE,
                        SCHEMA,
                        pathFactory,
                        () -> seqNumCounter,
                        COMPRESSION,
                        new StatsCollectorFactories(new CoreOptions(new Options())),
                        new FileIndexOptions(),
                        FileSource.APPEND,
                        false,
                        false,
                        null);
    }

    @Test
    public void testBasicWriting() throws IOException {
        // Write a single row
        writer.write(makeRows(1, 10).get(0));
        assertThat(writer.recordCount()).isEqualTo(1);
    }

    @Test
    public void testMultipleWrites() throws Exception {
        // Write multiple rows
        int rowNum = RANDOM.nextInt(64) + 1;
        writer.write(makeRows(rowNum, 10).iterator());
        writer.close();
        List<DataFileMeta> metasResult = writer.result();

        assertThat(metasResult.size()).isEqualTo(3); // blob is small, normal/blob/vector 3 files
        assertThat(metasResult.get(0).fileFormat()).isEqualTo("parquet");
        assertThat(metasResult.get(1).fileFormat()).isEqualTo("blob");
        assertThat(metasResult.get(2).fileFormat()).isEqualTo("json");
        assertThat(writer.recordCount()).isEqualTo(rowNum);

        assertThat(metasResult.get(0).rowCount()).isEqualTo(metasResult.get(1).rowCount());
        assertThat(metasResult.get(0).rowCount()).isEqualTo(metasResult.get(2).rowCount());
    }

    @Test
    public void testVectorStoreTargetFileSize() throws Exception {
        // 100k vector-store data would create 1 normal, 1 blob, and 3 vector-store files
        int rowNum = 100 * 1000;
        writer.write(makeRows(rowNum, 1).iterator());
        writer.close();
        List<DataFileMeta> results = writer.result();

        // Verify that we have multiple files due to rolling
        assertThat(results.size()).isGreaterThan(1);

        // Check that vector-store files meet the target size requirement
        List<DataFileMeta> vectorStoreFiles =
                results.stream()
                        .filter(file -> VectorStoreUtils.isVectorStoreFile(file.fileName()))
                        .collect(java.util.stream.Collectors.toList());

        assertThat(vectorStoreFiles.size()).isEqualTo(3);

        // Verify that vector-store files are close to the target size (within reasonable tolerance)
        for (DataFileMeta file : vectorStoreFiles.subList(0, vectorStoreFiles.size() - 1)) {
            long fileSize = file.fileSize();
            assertThat(fileSize)
                    .as("Vector-store file size should be close to target size")
                    .isGreaterThanOrEqualTo(VECTOR_STORE_TARGET_FILE_SIZE)
                    .isLessThanOrEqualTo(VECTOR_STORE_TARGET_FILE_SIZE + 256 * 1024);
        }

        // Verify total record count
        assertThat(writer.recordCount()).isEqualTo(rowNum);
    }

    @Test
    void testVectorStoreFileNameFormatWithSharedUuid() throws Exception {
        // 100k vector-store data would create 1 normal, 1 blob, and 3 vector-store files
        int rowNum = 100 * 1000;
        writer.write(makeRows(rowNum, 1).iterator());
        writer.close();
        List<DataFileMeta> results = writer.result();

        // Get uuid from vector-store files. The pattern is data-{uuid}-{count}.vector-store.json
        DataFileMeta oneVectorStoreFile =
                results.stream()
                        .filter(file -> VectorStoreUtils.isVectorStoreFile(file.fileName()))
                        .findAny()
                        .get();
        String uuidAndCnt = oneVectorStoreFile.fileName().split(".vector-store.")[0];
        String prefix = uuidAndCnt.substring(0, uuidAndCnt.lastIndexOf('-') + 1); // data-{uuid}-

        // Verify all files use the same UUID and have sequential counters
        for (int i = 0; i < results.size(); ++i) {
            String fileName = results.get(i).fileName();
            assertThat(fileName).as("All files should use the same UUID").startsWith(prefix);
            int counter = Integer.parseInt(fileName.substring(prefix.length()).split("\\.")[0]);
            assertThat(counter).as("File counter should be sequential").isEqualTo(i);
        }
    }

    @Test
    void testVectorStoreStatsMainPart() throws Exception {
        // Write multiple rows
        int rowNum = RANDOM.nextInt(64) + 1;
        writer.write(makeRows(rowNum, 10).iterator());
        writer.close();
        List<DataFileMeta> metasResult = writer.result();

        // Check row count
        for (DataFileMeta file : metasResult) {
            assertThat(file.rowCount()).isEqualTo(rowNum);
            assertThat(file.deleteRowCount().get()).isEqualTo(0); // There is no deleted rows
        }

        // Check statistics
        for (DataFileMeta file : metasResult) {
            if (BlobFileFormat.isBlobFile(file.fileName())) {
                assertThat(file.writeCols()).isEqualTo(Collections.singletonList("f2"));
            } else if (VectorStoreUtils.isVectorStoreFile(file.fileName())) {
                assertThat(file.writeCols()).isEqualTo(Arrays.asList("f3", "f4"));
                // Json does not implement createStatsExtractor so we skip it here.
                // assertThat(file.valueStats().minValues().getInt(1)).isGreaterThan(0);
            } else {
                assertThat(file.writeCols()).isEqualTo(Arrays.asList("f0", "f1"));
                assertThat(file.valueStats().minValues().getInt(0)).isEqualTo(0);
                assertThat(file.valueStats().maxValues().getInt(0)).isEqualTo(rowNum - 1);
            }
        }

        // Verify total record count
        assertThat(writer.recordCount()).isEqualTo(rowNum);
    }

    @Test
    void testVectorStoreStatsVectorStorePart() throws Exception {
        // This time we set parquet as vector-store file format.
        RowType schema =
                RowType.builder()
                        .field("f0", DataTypes.VECTOR(VECTOR_DIM, DataTypes.FLOAT()))
                        .field("f1", DataTypes.INT())
                        .field("f2", DataTypes.BLOB())
                        .field("f3", DataTypes.INT())
                        .field("f4", DataTypes.STRING())
                        .build();
        writer =
                new DataEvolutionRollingFileWriter(
                        LocalFileIO.create(),
                        SCHEMA_ID,
                        FileFormat.fromIdentifier("json", new Options()),
                        FileFormat.fromIdentifier("parquet", new Options()),
                        Arrays.asList("f3", "f4"),
                        TARGET_FILE_SIZE,
                        TARGET_FILE_SIZE,
                        VECTOR_STORE_TARGET_FILE_SIZE,
                        schema,
                        pathFactory,
                        () -> seqNumCounter,
                        COMPRESSION,
                        new StatsCollectorFactories(new CoreOptions(new Options())),
                        new FileIndexOptions(),
                        FileSource.APPEND,
                        false,
                        false,
                        null);

        // Write multiple rows
        int rowNum = RANDOM.nextInt(64) + 1;
        List<InternalRow> rows = makeRows(rowNum, 10);
        for (InternalRow row : rows) {
            writer.write(
                    GenericRow.of(
                            row.getVector(3),
                            row.getInt(4),
                            row.getBlob(2),
                            row.getInt(0),
                            row.getString(1)));
        }
        writer.close();
        List<DataFileMeta> metasResult = writer.result();

        // Check row count
        for (DataFileMeta file : metasResult) {
            assertThat(file.rowCount()).isEqualTo(rowNum);
            assertThat(file.deleteRowCount().get()).isEqualTo(0); // There is no deleted rows
        }

        // Check statistics
        for (DataFileMeta file : metasResult) {
            if (BlobFileFormat.isBlobFile(file.fileName())) {
                assertThat(file.writeCols()).isEqualTo(Collections.singletonList("f2"));
            } else if (VectorStoreUtils.isVectorStoreFile(file.fileName())) {
                assertThat(file.writeCols()).isEqualTo(Arrays.asList("f3", "f4"));
                assertThat(file.valueStats().minValues().getInt(0)).isEqualTo(0);
                assertThat(file.valueStats().maxValues().getInt(0)).isEqualTo(rowNum - 1);
            } else {
                assertThat(file.writeCols()).isEqualTo(Arrays.asList("f0", "f1"));
                // Json does not implement createStatsExtractor so we skip it here.
                // assertThat(file.valueStats().minValues().getInt(1)).isGreaterThan(0);
            }
        }

        // Verify total record count
        assertThat(writer.recordCount()).isEqualTo(rowNum);
    }

    @Test
    public void testVectorStoreNoBlob() throws Exception {
        RowType schema =
                RowType.builder()
                        .field("f0", DataTypes.INT())
                        .field("f1", DataTypes.STRING())
                        .field("f2", DataTypes.VECTOR(VECTOR_DIM, DataTypes.FLOAT()))
                        .field("f3", DataTypes.INT())
                        .build();
        writer =
                new DataEvolutionRollingFileWriter(
                        LocalFileIO.create(),
                        SCHEMA_ID,
                        FileFormat.fromIdentifier("parquet", new Options()),
                        FileFormat.fromIdentifier("json", new Options()),
                        Arrays.asList("f2", "f3"),
                        TARGET_FILE_SIZE,
                        TARGET_FILE_SIZE,
                        VECTOR_STORE_TARGET_FILE_SIZE,
                        schema,
                        pathFactory,
                        () -> seqNumCounter,
                        COMPRESSION,
                        new StatsCollectorFactories(new CoreOptions(new Options())),
                        new FileIndexOptions(),
                        FileSource.APPEND,
                        false,
                        false,
                        null);

        // 100k vector-store data would create 1 normal and 3 vector-store files
        int rowNum = 100 * 1000;
        List<InternalRow> rows = makeRows(rowNum, 1);
        for (InternalRow row : rows) {
            writer.write(
                    GenericRow.of(
                            row.getInt(0), row.getString(1), row.getVector(3), row.getInt(4)));
        }
        writer.close();
        List<DataFileMeta> results = writer.result();

        // Check normal, blob, and vector-store files
        List<DataFileMeta> normalFiles = new ArrayList<>();
        List<DataFileMeta> blobFiles = new ArrayList<>();
        List<DataFileMeta> vectorStoreFiles = new ArrayList<>();
        for (DataFileMeta file : results) {
            if (BlobFileFormat.isBlobFile(file.fileName())) {
                blobFiles.add(file);
            } else if (VectorStoreUtils.isVectorStoreFile(file.fileName())) {
                vectorStoreFiles.add(file);
            } else {
                normalFiles.add(file);
            }
        }
        assertThat(normalFiles.size()).isEqualTo(1);
        assertThat(blobFiles.size()).isEqualTo(0);
        assertThat(vectorStoreFiles.size()).isEqualTo(3);

        // Verify total record count
        assertThat(writer.recordCount()).isEqualTo(rowNum);
    }

    @Test
    public void testVectorStoreTheSameFormat() throws Exception {
        // vector-store file format is the same as main part
        writer =
                new DataEvolutionRollingFileWriter(
                        LocalFileIO.create(),
                        SCHEMA_ID,
                        FileFormat.fromIdentifier("json", new Options()),
                        FileFormat.fromIdentifier("json", new Options()),
                        Arrays.asList("f3", "f4"),
                        TARGET_FILE_SIZE,
                        TARGET_FILE_SIZE,
                        VECTOR_STORE_TARGET_FILE_SIZE,
                        SCHEMA,
                        pathFactory,
                        () -> seqNumCounter,
                        COMPRESSION,
                        new StatsCollectorFactories(new CoreOptions(new Options())),
                        new FileIndexOptions(),
                        FileSource.APPEND,
                        false,
                        false,
                        null);

        // This time we use large blob files
        int rowNum = 10;
        writer.write(makeRows(rowNum, 512 * 1024).iterator());
        writer.close();
        List<DataFileMeta> results = writer.result();

        // Check normal, blob, and vector-store files
        List<DataFileMeta> normalFiles = new ArrayList<>();
        List<DataFileMeta> blobFiles = new ArrayList<>();
        List<DataFileMeta> vectorStoreFiles = new ArrayList<>();
        for (DataFileMeta file : results) {
            if (BlobFileFormat.isBlobFile(file.fileName())) {
                blobFiles.add(file);
            } else if (VectorStoreUtils.isVectorStoreFile(file.fileName())) {
                vectorStoreFiles.add(file);
            } else {
                assertThat(file.writeCols()).isEqualTo(Arrays.asList("f0", "f1", "f3", "f4"));
                normalFiles.add(file);
            }
        }
        assertThat(normalFiles.size()).isEqualTo(1);
        assertThat(blobFiles.size()).isEqualTo(3);
        assertThat(vectorStoreFiles.size()).isEqualTo(0);

        // Verify total record count
        assertThat(writer.recordCount()).isEqualTo(rowNum);
    }

    private List<InternalRow> makeRows(int rowNum, int blobDataSize) {
        List<InternalRow> rows = new ArrayList<>(rowNum);
        byte[] blobData = new byte[blobDataSize];
        RANDOM.nextBytes(blobData);
        for (int i = 0; i < rowNum; ++i) {
            byte[] string = new byte[1];
            RANDOM.nextBytes(string);
            byte[] buf = new byte[VECTOR_DIM];
            RANDOM.nextBytes(buf);
            float[] vector = new float[VECTOR_DIM];
            for (int j = 0; j < VECTOR_DIM; ++j) {
                vector[j] = buf[j];
            }
            int label = RANDOM.nextInt(32) + 1;
            rows.add(
                    GenericRow.of(
                            i,
                            BinaryString.fromBytes(string),
                            new BlobData(blobData),
                            BinaryVector.fromPrimitiveArray(vector),
                            label));
        }
        return rows;
    }
}
