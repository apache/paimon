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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.testfulltext.TestFullTextGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FullTextSearchBuilder} using test-only brute-force full-text index. */
public class FullTextSearchBuilderTest extends TableTestBase {

    private static final String TEXT_FIELD_NAME = "content";

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column(TEXT_FIELD_NAME, DataTypes.STRING())
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .build();
    }

    @Test
    public void testFullTextSearchEndToEnd() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {
            "Apache Paimon is a lake format",
            "Paimon supports full-text search",
            "Vector search is also supported",
            "Paimon provides streaming and batch processing",
            "Full-text indexing enables fast text queries",
            "The lake format supports ACID transactions"
        };

        writeDocuments(table, documents);
        buildAndCommitIndex(table, documents);

        // Query "Paimon" - should match rows 0, 1, 3
        GlobalIndexResult result =
                table.newFullTextSearchBuilder()
                        .withQueryText("Paimon")
                        .withLimit(3)
                        .withTextColumn(TEXT_FIELD_NAME)
                        .executeLocal();

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result.results().isEmpty()).isFalse();

        // Read using the search result
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Integer> ids = new ArrayList<>();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).isNotEmpty();
        assertThat(ids.size()).isLessThanOrEqualTo(3);
        // Rows 0, 1, 3 contain "Paimon"
        assertThat(ids).containsAnyOf(0, 1, 3);
    }

    @Test
    public void testFullTextSearchMultiTermQuery() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {
            "Apache Paimon lake format",
            "Paimon full-text search support",
            "full-text search in Apache Paimon",
            "Vector search capabilities",
        };

        writeDocuments(table, documents);
        buildAndCommitIndex(table, documents);

        // Query "Paimon search" - row 2 matches both terms, rows 1 matches both, row 0 matches one
        GlobalIndexResult result =
                table.newFullTextSearchBuilder()
                        .withQueryText("Paimon search")
                        .withLimit(2)
                        .withTextColumn(TEXT_FIELD_NAME)
                        .executeLocal();

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result.results().isEmpty()).isFalse();

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).hasSize(2);
        // Rows 1 and 2 contain both "Paimon" and "search"
        assertThat(ids).contains(1, 2);
    }

    @Test
    public void testFullTextSearchEmptyResult() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        // Write data but no index - should return empty result
        String[] documents = {"hello world", "foo bar"};
        writeDocuments(table, documents);

        GlobalIndexResult result =
                table.newFullTextSearchBuilder()
                        .withQueryText("nonexistent")
                        .withLimit(1)
                        .withTextColumn(TEXT_FIELD_NAME)
                        .executeLocal();

        assertThat(result.results().isEmpty()).isTrue();
    }

    @Test
    public void testFullTextSearchTopKLimit() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = new String[20];
        for (int i = 0; i < 20; i++) {
            documents[i] = "document number " + i + " with common keyword";
        }

        writeDocuments(table, documents);
        buildAndCommitIndex(table, documents);

        // Search with limit=5
        GlobalIndexResult result =
                table.newFullTextSearchBuilder()
                        .withQueryText("keyword")
                        .withLimit(5)
                        .withTextColumn(TEXT_FIELD_NAME)
                        .executeLocal();

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids.size()).isLessThanOrEqualTo(5);
    }

    @Test
    public void testFullTextSearchWithMultipleIndexFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] allDocuments = {
            "Apache Paimon lake format", // row 0
            "Paimon supports streaming", // row 1
            "batch processing engine", // row 2
            "Paimon full-text search", // row 3
            "vector similarity search", // row 4
            "Paimon ACID transactions" // row 5
        };

        writeDocuments(table, allDocuments);

        // Build two separate index files covering different row ranges
        buildAndCommitMultipleIndexFiles(table, allDocuments);

        // Query "Paimon" - results should span across both index files
        GlobalIndexResult result =
                table.newFullTextSearchBuilder()
                        .withQueryText("Paimon")
                        .withLimit(4)
                        .withTextColumn(TEXT_FIELD_NAME)
                        .executeLocal();

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result.results().isEmpty()).isFalse();

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).isNotEmpty();
        // Rows 0,1 are in first index file, rows 3,5 are in second
        assertThat(ids).containsAnyOf(0, 1);
        assertThat(ids).containsAnyOf(3, 5);
    }

    @Test
    public void testFullTextSearchNoMatchingDocuments() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {
            "Apache Paimon lake format", "streaming batch processing", "ACID transactions support"
        };

        writeDocuments(table, documents);
        buildAndCommitIndex(table, documents);

        // Query a term that doesn't exist in any document
        GlobalIndexResult result =
                table.newFullTextSearchBuilder()
                        .withQueryText("nonexistent")
                        .withLimit(3)
                        .withTextColumn(TEXT_FIELD_NAME)
                        .executeLocal();

        assertThat(result.results().isEmpty()).isTrue();
    }

    @Test
    public void testFullTextSearchCaseInsensitive() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {
            "Apache PAIMON Lake Format", "paimon supports search", "Paimon Is Great"
        };

        writeDocuments(table, documents);
        buildAndCommitIndex(table, documents);

        // Query lowercase "paimon" should match all three (case-insensitive)
        GlobalIndexResult result =
                table.newFullTextSearchBuilder()
                        .withQueryText("paimon")
                        .withLimit(3)
                        .withTextColumn(TEXT_FIELD_NAME)
                        .executeLocal();

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).hasSize(3);
        assertThat(ids).contains(0, 1, 2);
    }

    // ====================== Helper methods ======================

    private void writeDocuments(FileStoreTable table, String[] documents) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < documents.length; i++) {
                write.write(GenericRow.of(i, BinaryString.fromString(documents[i])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void buildAndCommitIndex(FileStoreTable table, String[] documents) throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField textField = table.rowType().getField(TEXT_FIELD_NAME);

        GlobalIndexSingletonWriter writer =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestFullTextGlobalIndexerFactory.IDENTIFIER,
                                textField,
                                options);
        for (String doc : documents) {
            writer.write(doc);
        }
        List<ResultEntry> entries = writer.finish();

        Range rowRange = new Range(0, documents.length - 1);
        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        textField.id(),
                        TestFullTextGlobalIndexerFactory.IDENTIFIER,
                        entries);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void buildAndCommitMultipleIndexFiles(FileStoreTable table, String[] documents)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField textField = table.rowType().getField(TEXT_FIELD_NAME);
        int mid = documents.length / 2;

        // Build first index file covering rows [0, mid)
        GlobalIndexSingletonWriter writer1 =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestFullTextGlobalIndexerFactory.IDENTIFIER,
                                textField,
                                options);
        for (int i = 0; i < mid; i++) {
            writer1.write(documents[i]);
        }
        List<ResultEntry> entries1 = writer1.finish();
        Range rowRange1 = new Range(0, mid - 1);
        List<IndexFileMeta> indexFiles1 =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange1,
                        textField.id(),
                        TestFullTextGlobalIndexerFactory.IDENTIFIER,
                        entries1);

        // Build second index file covering rows [mid, end)
        GlobalIndexSingletonWriter writer2 =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestFullTextGlobalIndexerFactory.IDENTIFIER,
                                textField,
                                options);
        for (int i = mid; i < documents.length; i++) {
            writer2.write(documents[i]);
        }
        List<ResultEntry> entries2 = writer2.finish();
        Range rowRange2 = new Range(mid, documents.length - 1);
        List<IndexFileMeta> indexFiles2 =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange2,
                        textField.id(),
                        TestFullTextGlobalIndexerFactory.IDENTIFIER,
                        entries2);

        // Combine all index files and commit together
        List<IndexFileMeta> allIndexFiles = new ArrayList<>();
        allIndexFiles.addAll(indexFiles1);
        allIndexFiles.addAll(indexFiles2);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(allIndexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }
}
