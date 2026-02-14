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

package org.apache.paimon.diskann.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.diskann.DiskAnn;
import org.apache.paimon.diskann.DiskAnnException;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for scanning DiskANN vector global index. */
public class DiskAnnVectorGlobalIndexScanTest {

    @TempDir java.nio.file.Path tempDir;

    private FileStoreTable table;
    private String commitUser;
    private FileIO fileIO;
    private RowType rowType;
    private final String vectorFieldName = "vec";

    @BeforeEach
    public void before() throws Exception {
        // Skip tests if DiskANN native library is not available
        if (!DiskAnn.isLibraryLoaded()) {
            try {
                DiskAnn.loadLibrary();
            } catch (DiskAnnException e) {
                StringBuilder errorMsg = new StringBuilder("DiskANN native library not available.");
                errorMsg.append("\nError: ").append(e.getMessage());
                if (e.getCause() != null) {
                    errorMsg.append("\nCause: ").append(e.getCause().getMessage());
                }
                errorMsg.append(
                        "\n\nTo run DiskANN tests, ensure the paimon-diskann-jni JAR"
                                + " with native libraries is available in the classpath.");
                Assumptions.assumeTrue(false, errorMsg.toString());
            }
        }

        Path tablePath = new Path(tempDir.toString());
        fileIO = new LocalFileIO();
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(vectorFieldName, new ArrayType(DataTypes.FLOAT()))
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option("vector.dim", "2")
                        .option("vector.metric", "L2")
                        .option("data-evolution.enabled", "true")
                        .option("row-tracking.enabled", "true")
                        .build();

        TableSchema tableSchema = schemaManager.createTable(schema);
        table = FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
        rowType = table.rowType();
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testVectorIndexScanEndToEnd() throws Exception {
        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f}, new float[] {0.95f, 0.1f}, new float[] {0.1f, 0.95f},
                    new float[] {0.98f, 0.05f}, new float[] {0.0f, 1.0f}, new float[] {0.05f, 0.98f}
                };

        writeVectors(vectors);

        List<IndexFileMeta> indexFiles = buildIndexManually(vectors);

        commitIndex(indexFiles);

        float[] queryVector = new float[] {0.85f, 0.15f};
        VectorSearch vectorSearch = new VectorSearch(queryVector, 2, vectorFieldName);
        ReadBuilder readBuilder = table.newReadBuilder().withVectorSearch(vectorSearch);
        TableScan scan = readBuilder.newScan();
        List<Integer> ids = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(scan.plan())
                .forEachRemaining(
                        row -> {
                            ids.add(row.getInt(0));
                        });
        // With L2 distance, the closest vectors to [0.85, 0.15] should be [0.95, 0.1] and [0.98,
        // 0.05]
        assertThat(ids).containsExactlyInAnyOrder(1, 3);
    }

    @Test
    public void testVectorIndexScanWithDifferentMetrics() throws Exception {
        Path tablePath = new Path(tempDir.toString(), "inner_product");
        fileIO.mkdirs(tablePath);
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(vectorFieldName, new ArrayType(DataTypes.FLOAT()))
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option("vector.dim", "2")
                        .option("vector.metric", "INNER_PRODUCT")
                        .option("data-evolution.enabled", "true")
                        .option("row-tracking.enabled", "true")
                        .build();

        TableSchema tableSchema = schemaManager.createTable(schema);
        FileStoreTable ipTable = FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
        String ipCommitUser = UUID.randomUUID().toString();

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.707f, 0.707f},
                    new float[] {0.0f, 1.0f},
                };

        StreamTableWrite write = ipTable.newWrite(ipCommitUser);
        for (int i = 0; i < vectors.length; i++) {
            write.write(GenericRow.of(i, new GenericArray(vectors[i])));
        }
        List<CommitMessage> messages = write.prepareCommit(false, 0);
        StreamTableCommit commit = ipTable.newCommit(ipCommitUser);
        commit.commit(0, messages);
        write.close();

        Options options = new Options(ipTable.options());
        DiskAnnVectorIndexOptions indexOptions = new DiskAnnVectorIndexOptions(options);
        Path indexDir = ipTable.store().pathFactory().indexPath();
        if (!fileIO.exists(indexDir)) {
            fileIO.mkdirs(indexDir);
        }

        GlobalIndexFileWriter fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + "-" + UUID.randomUUID();
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(new Path(indexDir, fileName), false);
                    }
                };

        DiskAnnVectorGlobalIndexWriter indexWriter =
                new DiskAnnVectorGlobalIndexWriter(
                        fileWriter, new ArrayType(DataTypes.FLOAT()), indexOptions);
        for (float[] vec : vectors) {
            indexWriter.write(vec);
        }

        List<ResultEntry> entries = indexWriter.finish();
        List<IndexFileMeta> metas = new ArrayList<>();
        int fieldId = ipTable.rowType().getFieldIndex(vectorFieldName);

        for (ResultEntry entry : entries) {
            long fileSize = fileIO.getFileSize(new Path(indexDir, entry.fileName()));
            GlobalIndexMeta globalMeta =
                    new GlobalIndexMeta(0, vectors.length - 1, fieldId, null, entry.meta());

            metas.add(
                    new IndexFileMeta(
                            DiskAnnVectorGlobalIndexerFactory.IDENTIFIER,
                            entry.fileName(),
                            fileSize,
                            entry.rowCount(),
                            globalMeta,
                            (String) null));
        }

        DataIncrement dataIncrement = DataIncrement.indexIncrement(metas);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        ipTable.newCommit(ipCommitUser).commit(1, Collections.singletonList(message));

        float[] queryVector = new float[] {1.0f, 0.0f};
        VectorSearch vectorSearch = new VectorSearch(queryVector, 1, vectorFieldName);
        ReadBuilder readBuilder = ipTable.newReadBuilder().withVectorSearch(vectorSearch);
        TableScan scan = readBuilder.newScan();
        List<Integer> ids = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(scan.plan())
                .forEachRemaining(
                        row -> {
                            ids.add(row.getInt(0));
                        });
        assertThat(ids).containsExactly(0);
    }

    private void writeVectors(float[][] vectors) throws Exception {
        StreamTableWrite write = table.newWrite(commitUser);
        for (int i = 0; i < vectors.length; i++) {
            write.write(GenericRow.of(i, new GenericArray(vectors[i])));
        }
        List<CommitMessage> messages = write.prepareCommit(false, 0);
        StreamTableCommit commit = table.newCommit(commitUser);
        commit.commit(0, messages);
        write.close();
    }

    private List<IndexFileMeta> buildIndexManually(float[][] vectors) throws Exception {
        Options options = new Options(table.options());
        DiskAnnVectorIndexOptions indexOptions = new DiskAnnVectorIndexOptions(options);
        Path indexDir = table.store().pathFactory().indexPath();
        if (!fileIO.exists(indexDir)) {
            fileIO.mkdirs(indexDir);
        }

        GlobalIndexFileWriter fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + "-" + UUID.randomUUID();
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(new Path(indexDir, fileName), false);
                    }
                };

        DiskAnnVectorGlobalIndexWriter writer =
                new DiskAnnVectorGlobalIndexWriter(
                        fileWriter, new ArrayType(DataTypes.FLOAT()), indexOptions);
        for (float[] vec : vectors) {
            writer.write(vec);
        }

        List<ResultEntry> entries = writer.finish();

        List<IndexFileMeta> metas = new ArrayList<>();
        int fieldId = rowType.getFieldIndex(vectorFieldName);

        for (ResultEntry entry : entries) {
            long fileSize = fileIO.getFileSize(new Path(indexDir, entry.fileName()));
            GlobalIndexMeta globalMeta =
                    new GlobalIndexMeta(0, vectors.length - 1, fieldId, null, entry.meta());

            metas.add(
                    new IndexFileMeta(
                            DiskAnnVectorGlobalIndexerFactory.IDENTIFIER,
                            entry.fileName(),
                            fileSize,
                            entry.rowCount(),
                            globalMeta,
                            (String) null));
        }
        return metas;
    }

    private void commitIndex(List<IndexFileMeta> indexFiles) {
        StreamTableCommit commit = table.newCommit(commitUser);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        commit.commit(1, Collections.singletonList(message));
    }
}
