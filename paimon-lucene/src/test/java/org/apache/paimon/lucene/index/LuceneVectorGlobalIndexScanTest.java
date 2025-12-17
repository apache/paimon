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

package org.apache.paimon.lucene.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexScanBuilder;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.RowRangeGlobalIndexScanner;
import org.apache.paimon.globalindex.TopkGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for scanning Lucene vector global index. */
public class LuceneVectorGlobalIndexScanTest {

    @TempDir java.nio.file.Path tempDir;

    private FileStoreTable table;
    private String commitUser;
    private Path tablePath;
    private FileIO fileIO;
    private RowType rowType;
    private String similarityMetric = "EUCLIDEAN";
    private String vectorFieldName = "vec";

    @BeforeEach
    public void before() throws Exception {
        tablePath = new Path(tempDir.toString());
        fileIO = new LocalFileIO();
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);

        // 1. Define Schema, including vector field (FLOAT ARRAY)
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(vectorFieldName, new ArrayType(DataTypes.FLOAT()))
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option("vector.dim", "2")
                        .option("vector.metric", similarityMetric)
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

        FieldRef fieldRef = new FieldRef(1, vectorFieldName, new ArrayType(DataTypes.FLOAT()));

        // 3. Manually build vector index
        List<IndexFileMeta> indexFiles = buildIndexManually(vectors);

        // 4. Commit index files to the Table (Update Index Manifest)
        commitIndex(indexFiles);

        // 5. Use GlobalIndexScanBuilder to get scanner
        GlobalIndexScanBuilder scanBuilder = table.store().newGlobalIndexScanBuilder();
        List<Range> ranges = scanBuilder.shardList();
        Range range = ranges.get(0);
        RowRangeGlobalIndexScanner scanner = scanBuilder.withRowRange(range).build();

        // 6. Execute TopK query
        float[] queryVector = new float[] {0.85f, 0.15f};
        VectorSearch vectorSearch = new VectorSearch(queryVector, 2);

        // 7. Verify results without filter

        TopkGlobalIndexResult result =
                (TopkGlobalIndexResult) scanner.scan(fieldRef, vectorSearch).get();
        List<Long> resultRowIds = new ArrayList<>();
        result.results().iterator().forEachRemaining(resultRowIds::add);
        assertThat(resultRowIds).hasSize(2);

        // 8. Verify results with filter
        RoaringNavigableMap64 filterResults = new RoaringNavigableMap64();
        filterResults.add(1L);
        vectorSearch = new VectorSearch(queryVector, 2, filterResults.iterator());
        result = (TopkGlobalIndexResult) scanner.scan(fieldRef, vectorSearch).get();
        resultRowIds = new ArrayList<>();
        result.results().iterator().forEachRemaining(resultRowIds::add);
        float score = result.scoreGetter().score(resultRowIds.get(0));
        assertThat(resultRowIds).contains(1L);
        assertThat(score).isEqualTo(0.98765427f);
    }

    private List<IndexFileMeta> buildIndexManually(float[][] vectors) throws Exception {

        Options options = new Options(table.options());
        LuceneVectorIndexOptions indexOptions = new LuceneVectorIndexOptions(options);
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
                    public OutputStream newOutputStream(String fileName) throws IOException {
                        return fileIO.newOutputStream(new Path(indexDir, fileName), false);
                    }
                };

        LuceneVectorGlobalIndexWriter writer =
                new LuceneVectorGlobalIndexWriter(
                        fileWriter, new ArrayType(DataTypes.FLOAT()), indexOptions);
        for (float[] vec : vectors) {
            writer.write(vec);
        }

        List<GlobalIndexWriter.ResultEntry> entries = writer.finish();

        List<IndexFileMeta> metas = new ArrayList<>();
        int fieldId = rowType.getFieldIndex(vectorFieldName);

        for (GlobalIndexWriter.ResultEntry entry : entries) {
            long fileSize = fileIO.getFileSize(new Path(indexDir, entry.fileName()));
            GlobalIndexMeta globalMeta =
                    new GlobalIndexMeta(
                            entry.rowRange().from,
                            entry.rowRange().to,
                            fieldId,
                            null,
                            entry.meta());

            metas.add(
                    new IndexFileMeta(
                            LuceneVectorGlobalIndexerFactory.IDENTIFIER,
                            entry.fileName(),
                            fileSize,
                            entry.rowRange().to - entry.rowRange().from + 1,
                            globalMeta));
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
        commit.commit(0, Collections.singletonList(message));
    }
}
