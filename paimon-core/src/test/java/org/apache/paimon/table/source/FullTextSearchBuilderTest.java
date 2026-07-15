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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.globalindex.testfulltext.TestFullTextGlobalIndexerFactory;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
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
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.table.source.DeletionVectorTestUtils.commitDeletionVectors;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                        .withQuery(TEXT_FIELD_NAME, matchQuery("Paimon"))
                        .withLimit(3)
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
    public void testFullTextSearchExcludesDeletedIndexedRows() throws Exception {
        Identifier identifier = identifier("full_text_deleted_indexed_rows");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(TEXT_FIELD_NAME, DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        String[] documents = {
            "paimon keyword", "paimon keyword", "paimon keyword", "paimon keyword"
        };
        writeDocuments(table, documents);
        buildAndCommitIndex(table, documents);
        commitDeletionVectors(table, 0L, 1L);

        GlobalIndexResult result =
                table.newFullTextSearchBuilder()
                        .withQuery(TEXT_FIELD_NAME, matchQuery("keyword"))
                        .withLimit(2)
                        .executeLocal();

        assertThat(result.results().getLongCardinality()).isEqualTo(2);
        assertThat(result.results()).contains(2L, 3L);
        assertThat(result.results()).doesNotContain(0L, 1L);
        assertThat(readIds(table, result)).containsExactlyInAnyOrder(2, 3);
    }

    @Test
    public void testFullTextSearchNonFastModesScanUnindexedData() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] indexedDocuments = {
            "Apache Paimon is a lake format", "Paimon supports full-text search"
        };
        writeDocuments(table, indexedDocuments);
        buildAndCommitIndex(table, indexedDocuments);
        writeDocuments(
                table,
                new String[] {
                    "Vector search is also supported", "Fresh Paimon documents should be searchable"
                });

        GlobalIndexResult fastResult =
                table.newFullTextSearchBuilder()
                        .withQuery(TEXT_FIELD_NAME, matchQuery("Fresh"))
                        .withLimit(10)
                        .executeLocal();
        assertThat(readIds(table, fastResult)).isEmpty();

        for (String searchMode : Arrays.asList("full", "detail")) {
            FileStoreTable nonFastModeTable =
                    (FileStoreTable)
                            table.copy(
                                    Collections.singletonMap(
                                            CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(),
                                            searchMode));
            GlobalIndexResult result =
                    nonFastModeTable
                            .newFullTextSearchBuilder()
                            .withQuery(TEXT_FIELD_NAME, matchQuery("Fresh"))
                            .withLimit(10)
                            .executeLocal();

            assertThat(readIds(nonFastModeTable, result)).containsExactly(1);
        }
    }

    @Test
    public void testFullTextSearchRawSearchRespectsPartitionFilter() throws Exception {
        Identifier identifier = identifier("PartitionedTextTable");
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("id", DataTypes.INT())
                        .column(TEXT_FIELD_NAME, DataTypes.STRING())
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        RowType partitionType = RowType.of(DataTypes.INT());
        InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
        BinaryRow partition1 = serializer.toBinaryRow(GenericRow.of(1)).copy();

        writePartitionedDocuments(
                table, 1, new String[] {"indexed Paimon document", "another document"});
        buildAndCommitIndexForColumn(
                table,
                TEXT_FIELD_NAME,
                new String[] {"indexed Paimon document", "another document"},
                partition1);
        writePartitionedDocuments(table, 2, new String[] {"fresh Paimon document"});

        PartitionPredicate partitionFilter =
                PartitionPredicate.fromMultiple(
                        partitionType, Collections.singletonList(partition1));
        FileStoreTable fullModeTable =
                (FileStoreTable)
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "full"));

        GlobalIndexResult result =
                fullModeTable
                        .newFullTextSearchBuilder()
                        .withPartitionFilter(partitionFilter)
                        .withQuery(TEXT_FIELD_NAME, matchQuery("fresh"))
                        .withLimit(10)
                        .executeLocal();

        assertThat(readIds(fullModeTable, result)).isEmpty();
    }

    @Test
    public void testHybridSearchBuilderWithFullTextRoute() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {
            "Apache Paimon is a lake format",
            "Paimon supports full-text search",
            "Vector search is also supported"
        };

        writeDocuments(table, documents);
        buildAndCommitIndex(table, documents);

        ScoredGlobalIndexResult result =
                table.newHybridSearchBuilder()
                        .addFullTextRoute(TEXT_FIELD_NAME, matchQuery("Paimon"), 3, 1.0f)
                        .withLimit(3)
                        .executeLocal();

        assertThat(result.results().isEmpty()).isFalse();
        assertThat(result.scoreGetter().score(result.results().iterator().next())).isGreaterThan(0);
    }

    @Test
    public void testHybridSearchRejectsDataFilterWithFullTextRoute() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        Predicate idFilter = new PredicateBuilder(table.rowType()).equal(0, 1);

        assertThatThrownBy(
                        () ->
                                table.newHybridSearchBuilder()
                                        .addFullTextRoute(
                                                TEXT_FIELD_NAME, matchQuery("Paimon"), 3, 1.0f)
                                        .withFilter(idFilter)
                                        .withLimit(3)
                                        .routeBuilders())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support non-partition filters");
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
                        .withQuery(TEXT_FIELD_NAME, matchQuery("Paimon search"))
                        .withLimit(2)
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
    public void testStructuredFullTextSearchPhraseAndBooleanQuery() throws Exception {
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

        GlobalIndexResult phraseResult =
                table.newFullTextSearchBuilder()
                        .withQuery(TEXT_FIELD_NAME, phraseQuery("full-text search"))
                        .withLimit(10)
                        .executeLocal();

        assertThat(readIds(table, phraseResult)).containsExactlyInAnyOrder(1, 2);

        GlobalIndexResult booleanResult =
                table.newFullTextSearchBuilder()
                        .withQuery(
                                TEXT_FIELD_NAME,
                                booleanMustShouldNotQuery("Paimon", "search", "Vector"))
                        .withLimit(10)
                        .executeLocal();

        // Native boolean search treats should clauses as optional when must clauses exist.
        assertThat(readIds(table, booleanResult)).containsExactlyInAnyOrder(0, 1, 2);
    }

    @Test
    public void testCompoundFullTextSearchUsesFullLeafCandidatesBeforeFinalTopK() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {"paimon vector", "paimon", "paimon", "paimon", "paimon", "paimon"};

        writeDocuments(table, documents);
        buildAndCommitIndex(table, documents);

        ScoredGlobalIndexResult result =
                (ScoredGlobalIndexResult)
                        table.newFullTextSearchBuilder()
                                .withQuery(TEXT_FIELD_NAME, boostQuery("paimon", "vector", 0.1f))
                                .withLimit(3)
                                .executeLocal();

        assertThat(readIds(table, result)).doesNotContain(0);
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
                        .withQuery(TEXT_FIELD_NAME, matchQuery("nonexistent"))
                        .withLimit(1)
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
                        .withQuery(TEXT_FIELD_NAME, matchQuery("keyword"))
                        .withLimit(5)
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
                        .withQuery(TEXT_FIELD_NAME, matchQuery("Paimon"))
                        .withLimit(4)
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
    public void testFullTextSearchIgnoresOtherIndexTypesOnSameColumn() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {"Apache Paimon lake format", "vector search"};

        writeDocuments(table, documents);
        buildAndCommitIndex(table, documents);
        buildAndCommitBTreeIndex(table, documents);

        GlobalIndexResult result =
                table.newFullTextSearchBuilder()
                        .withQuery(TEXT_FIELD_NAME, matchQuery("Paimon"))
                        .withLimit(10)
                        .executeLocal();

        assertThat(readIds(table, result)).containsExactly(0);
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
                        .withQuery(TEXT_FIELD_NAME, matchQuery("nonexistent"))
                        .withLimit(3)
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
                        .withQuery(TEXT_FIELD_NAME, matchQuery("paimon"))
                        .withLimit(3)
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

    @Test
    public void testFullTextSearchServesTextColumnAsExtraField() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {"Apache Paimon", "vector search"};
        writeDocuments(table, documents);
        // Multi-column index: primary is "id", the text column is an EXTRA field. Full-text search
        // on the extra text column is served (matched via extraFieldIds).
        buildAndCommitIndexWithFields(
                table,
                documents,
                Arrays.asList(
                        table.rowType().getField("id"), table.rowType().getField(TEXT_FIELD_NAME)));

        FullTextSearchBuilder searchBuilder =
                table.newFullTextSearchBuilder()
                        .withQuery(TEXT_FIELD_NAME, matchQuery("Paimon"))
                        .withLimit(2);

        assertThat(searchBuilder.newFullTextScan().scan().splits()).isNotEmpty();
        assertThat(searchBuilder.executeLocal().results().isEmpty()).isFalse();
    }

    @Test
    public void testFullTextSearchPrefersDedicatedIndexOverExtraFieldCoverage() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {"Apache Paimon", "vector search"};
        writeDocuments(table, documents);

        // Two full-text-capable indexes cover the same text column over the same row range:
        //  (a) a dedicated full-text index whose PRIMARY field is the text column, and
        //  (b) a multi-column index whose primary is "id" and carries the text column as an EXTRA
        //      field.
        // These are different index definitions and must not be merged into one reader input; the
        // dedicated index (a) takes precedence, so each split carries a single index file.
        buildAndCommitIndex(table, documents); // (a) primary = content
        buildAndCommitIndexWithFields(
                table,
                documents,
                Arrays.asList(
                        table.rowType().getField("id"),
                        table.rowType().getField(TEXT_FIELD_NAME))); // (b) content as extra

        FullTextSearchBuilder searchBuilder =
                table.newFullTextSearchBuilder()
                        .withQuery(TEXT_FIELD_NAME, matchQuery("Paimon"))
                        .withLimit(2);

        List<FullTextSearchSplit> splits = searchBuilder.newFullTextScan().scan().splits();
        assertThat(splits).isNotEmpty();
        int textFieldId = table.rowType().getField(TEXT_FIELD_NAME).id();
        for (FullTextSearchSplit split : splits) {
            IndexFullTextSearchSplit indexSplit = (IndexFullTextSearchSplit) split;
            // Files from different index definitions are never merged into one split ...
            assertThat(indexSplit.fullTextIndexFiles()).hasSize(1);
            // ... and the chosen definition is the dedicated one (text column is its primary
            // field).
            assertThat(indexSplit.fullTextIndexFiles().get(0).globalIndexMeta().indexFieldId())
                    .isEqualTo(textFieldId);
        }
        assertThat(searchBuilder.executeLocal().results().isEmpty()).isFalse();
    }

    @Test
    public void testFullTextSearchKeepsRangeCoveredOnlyByExtraFieldIndex() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {"paimon lake", "vector search", "paimon engine", "streaming"};
        writeDocuments(table, documents);

        // Two index definitions cover DIFFERENT row ranges of the same text column:
        //  - a dedicated full-text index (primary field = content) over rows [0, 1], and
        //  - a multi-column index (primary = id, content as an extra field) over rows [2, 3].
        // Per-column selection would keep only the dedicated identity and drop the extra-field
        // index's [2, 3] file, while raw coverage (computed from all index files) still treats
        // [2, 3] as indexed, so row 2 ("paimon engine") would be silently unsearchable. Per-range
        // selection must keep both ranges searchable.
        buildAndCommitIndexRange(
                table,
                new String[] {"paimon lake", "vector search"},
                Collections.singletonList(table.rowType().getField(TEXT_FIELD_NAME)),
                0);
        buildAndCommitIndexRange(
                table,
                new String[] {"paimon engine", "streaming"},
                Arrays.asList(
                        table.rowType().getField("id"), table.rowType().getField(TEXT_FIELD_NAME)),
                2);

        GlobalIndexResult result =
                table.newFullTextSearchBuilder()
                        .withQuery(TEXT_FIELD_NAME, matchQuery("paimon"))
                        .withLimit(10)
                        .executeLocal();

        // Row 0 is served by the dedicated index over [0,1]; row 2 by the extra-field index over
        // [2,3]. Neither range may be dropped.
        assertThat(readIds(table, result)).containsExactlyInAnyOrder(0, 2);
    }

    @Test
    public void testFullTextSearchAssignsOverlappingRangesOnlyOnce() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {"needle zero", "other", "needle two", "needle three"};
        writeDocuments(table, documents);

        // The dedicated index covers [0, 2], while an index carrying content as an extra field
        // covers [2, 3]. Row 2 is in both physical files, but must be assigned only to the
        // dedicated index. The second file is still read with its physical [2, 3] offset and an
        // include mask for its assigned tail [3, 3].
        buildAndCommitIndexRange(
                table,
                new String[] {documents[0], documents[1], documents[2]},
                Collections.singletonList(table.rowType().getField(TEXT_FIELD_NAME)),
                0);
        buildAndCommitIndexRange(
                table,
                new String[] {documents[2], documents[3]},
                Arrays.asList(
                        table.rowType().getField("id"), table.rowType().getField(TEXT_FIELD_NAME)),
                2);

        FullTextSearchBuilder searchBuilder =
                table.newFullTextSearchBuilder()
                        .withQuery(TEXT_FIELD_NAME, matchQuery("needle"))
                        .withLimit(10);
        List<IndexFullTextSearchSplit> indexSplits = new ArrayList<>();
        for (FullTextSearchSplit split : searchBuilder.newFullTextScan().scan().splits()) {
            indexSplits.add((IndexFullTextSearchSplit) split);
        }

        assertThat(indexSplits).hasSize(2);
        assertThat(indexSplits.get(0).rowRangeStart()).isEqualTo(0);
        assertThat(indexSplits.get(0).rowRangeEnd()).isEqualTo(2);
        assertThat(indexSplits.get(0).searchRowRanges()).containsExactly(new Range(0, 2));
        assertThat(indexSplits.get(1).rowRangeStart()).isEqualTo(2);
        assertThat(indexSplits.get(1).rowRangeEnd()).isEqualTo(3);
        assertThat(indexSplits.get(1).searchRowRanges()).containsExactly(new Range(3, 3));

        assertThat(readIds(table, searchBuilder.executeLocal())).containsExactlyInAnyOrder(0, 2, 3);
    }

    @Test
    public void testFullTextIndexIdentityPreservesExtraFieldOrder() {
        IndexFileMeta titleThenBody =
                new IndexFileMeta(
                        TestFullTextGlobalIndexerFactory.IDENTIFIER,
                        "title-body",
                        1,
                        2,
                        new GlobalIndexMeta(0, 1, 0, new int[] {1, 2}, null),
                        null);
        IndexFileMeta bodyThenTitle =
                new IndexFileMeta(
                        TestFullTextGlobalIndexerFactory.IDENTIFIER,
                        "body-title",
                        1,
                        2,
                        new GlobalIndexMeta(0, 1, 0, new int[] {2, 1}, null),
                        null);

        // The reader's projected row layout follows this order, so these are different index
        // definitions even though they contain the same field-id set.
        assertThat(DataEvolutionFullTextScan.sameIndexIdentity(titleThenBody, bodyThenTitle))
                .isFalse();
    }

    @Test
    public void testFullTextSearchSkipsIndexNotCoveringTextColumn() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {"Apache Paimon", "vector search"};
        writeDocuments(table, documents);
        // The index covers only "id" — the text column is neither the primary nor an extra field —
        // so full-text search on the text column finds no index and returns empty.
        buildAndCommitIndexWithFields(
                table, documents, Arrays.asList(table.rowType().getField("id")));

        FullTextSearchBuilder searchBuilder =
                table.newFullTextSearchBuilder()
                        .withQuery(TEXT_FIELD_NAME, matchQuery("Paimon"))
                        .withLimit(2);

        FullTextScan.Plan plan = searchBuilder.newFullTextScan().scan();
        assertThat(plan.splits()).isEmpty();
        assertThat(searchBuilder.executeLocal().results().isEmpty()).isTrue();
    }

    @Test
    public void testOrdinaryFullTextScanSkipsSourceBackedPrimaryKeyArchive() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {"Apache Paimon", "vector search"};
        writeDocuments(table, documents);
        buildAndCommitSourceBackedIndex(table, documents);

        FullTextScan.Plan plan =
                table.newFullTextSearchBuilder()
                        .withQuery(TEXT_FIELD_NAME, matchQuery("Paimon"))
                        .withLimit(2)
                        .newFullTextScan()
                        .scan();

        assertThat(plan.splits()).isEmpty();
    }

    @Test
    public void testFullTextSearchSplitSerialization() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        String[] documents = {"Apache Paimon", "full-text search"};
        writeDocuments(table, documents);
        buildAndCommitIndex(table, documents);

        FullTextScan.Plan plan =
                table.newFullTextSearchBuilder()
                        .withQuery(TEXT_FIELD_NAME, matchQuery("Paimon"))
                        .withLimit(2)
                        .newFullTextScan()
                        .scan();

        assertThat(plan.splits()).hasSize(1);
        IndexFullTextSearchSplit original = (IndexFullTextSearchSplit) plan.splits().get(0);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(original);
        }

        IndexFullTextSearchSplit deserialized;
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            deserialized = (IndexFullTextSearchSplit) in.readObject();
        }

        assertThat(deserialized.columnName()).isEqualTo(original.columnName());
        assertThat(deserialized.rowRangeStart()).isEqualTo(original.rowRangeStart());
        assertThat(deserialized.rowRangeEnd()).isEqualTo(original.rowRangeEnd());
        assertThat(deserialized.searchRowRanges()).isEqualTo(original.searchRowRanges());
        assertThat(deserialized.fullTextIndexFiles()).hasSize(original.fullTextIndexFiles().size());
        for (int i = 0; i < original.fullTextIndexFiles().size(); i++) {
            assertThat(deserialized.fullTextIndexFiles().get(i).fileName())
                    .isEqualTo(original.fullTextIndexFiles().get(i).fileName());
        }

        RawFullTextSearchSplit rawOriginal =
                new RawFullTextSearchSplit(Collections.singletonList(new Range(2, 3)));
        bos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(rawOriginal);
        }

        RawFullTextSearchSplit rawDeserialized;
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            rawDeserialized = (RawFullTextSearchSplit) in.readObject();
        }

        assertThat(rawDeserialized.rowRanges()).isEqualTo(rawOriginal.rowRanges());
    }

    // ====================== Helper methods ======================

    private static String matchQuery(String terms) {
        return "{\"match\":{\"query\":\"" + terms + "\"}}";
    }

    private static String phraseQuery(String terms) {
        return "{\"match_phrase\":{\"query\":\"" + terms + "\"}}";
    }

    private static String booleanMustShouldNotQuery(String must, String should, String mustNot) {
        return "{\"boolean\":{\"queries\":["
                + "[\"Must\","
                + matchQuery(must)
                + "],"
                + "[\"Should\","
                + matchQuery(should)
                + "],"
                + "[\"MustNot\","
                + matchQuery(mustNot)
                + "]]}}";
    }

    private static String boostQuery(String positive, String negative, float negativeBoost) {
        return "{\"boost\":{\"positive\":"
                + matchQuery(positive)
                + ",\"negative\":"
                + matchQuery(negative)
                + ",\"negative_boost\":"
                + negativeBoost
                + "}}";
    }

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

    private void writePartitionedDocuments(FileStoreTable table, int partition, String[] documents)
            throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < documents.length; i++) {
                write.write(GenericRow.of(partition, i, BinaryString.fromString(documents[i])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void buildAndCommitIndex(FileStoreTable table, String[] documents) throws Exception {
        buildAndCommitIndexWithFields(
                table,
                documents,
                Collections.singletonList(table.rowType().getField(TEXT_FIELD_NAME)));
    }

    private void buildAndCommitIndexWithFields(
            FileStoreTable table, String[] documents, List<DataField> indexFields)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField textField = table.rowType().getField(TEXT_FIELD_NAME);

        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestFullTextGlobalIndexerFactory.IDENTIFIER,
                                textField,
                                options);
        for (int i = 0; i < documents.length; i++) {
            writer.write(documents[i], i);
        }
        List<ResultEntry> entries = writer.finish();

        Range rowRange = new Range(0, documents.length - 1);
        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        indexFields,
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

    private void buildAndCommitSourceBackedIndex(FileStoreTable table, String[] documents)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField textField = table.rowType().getField(TEXT_FIELD_NAME);
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestFullTextGlobalIndexerFactory.IDENTIFIER,
                                textField,
                                options);
        for (int i = 0; i < documents.length; i++) {
            writer.write(documents[i], i);
        }

        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        new Range(0, documents.length - 1),
                        Collections.singletonList(textField),
                        TestFullTextGlobalIndexerFactory.IDENTIFIER,
                        writer.finish());
        byte[] sourceMeta =
                new PrimaryKeyIndexSourceMeta(
                                new PrimaryKeyIndexSourceFile("data-file", documents.length))
                        .serialize();
        List<IndexFileMeta> sourceBackedFiles = new ArrayList<>();
        for (IndexFileMeta indexFile : indexFiles) {
            GlobalIndexMeta meta = indexFile.globalIndexMeta();
            sourceBackedFiles.add(
                    new IndexFileMeta(
                            indexFile.indexType(),
                            indexFile.fileName(),
                            indexFile.fileSize(),
                            indexFile.rowCount(),
                            new GlobalIndexMeta(
                                    meta.rowRangeStart(),
                                    meta.rowRangeEnd(),
                                    meta.indexFieldId(),
                                    meta.extraFieldIds(),
                                    meta.indexMeta(),
                                    sourceMeta),
                            indexFile.externalPath()));
        }

        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        DataIncrement.indexIncrement(sourceBackedFiles),
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    /**
     * Builds and commits a single full-text index file covering rows [rowStart, rowStart+N-1].
     * {@code indexFields} determines the index identity (primary = first field, the rest are extra
     * fields), letting a test place different index definitions over different row ranges.
     */
    private void buildAndCommitIndexRange(
            FileStoreTable table, String[] documents, List<DataField> indexFields, int rowStart)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField textField = table.rowType().getField(TEXT_FIELD_NAME);

        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestFullTextGlobalIndexerFactory.IDENTIFIER,
                                textField,
                                options);
        // Doc ids are file-local (0-based); the global row offset is carried by rowRange.from,
        // which
        // the OffsetGlobalIndexReader adds back when mapping local doc ids to global row ids.
        for (int i = 0; i < documents.length; i++) {
            writer.write(documents[i], i);
        }
        List<ResultEntry> entries = writer.finish();

        Range rowRange = new Range(rowStart, rowStart + documents.length - 1);
        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        indexFields,
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

    private FileStoreTable createMultiTextTable() throws Exception {
        Identifier identifier = identifier("MultiTextTable");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("title", DataTypes.STRING())
                        .column("body", DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier, schema, true);
        return getTable(identifier);
    }

    private void writeMultiTextDocuments(FileStoreTable table, String[][] documents)
            throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < documents.length; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString(documents[i][0]),
                                BinaryString.fromString(documents[i][1])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void buildAndCommitIndexForColumn(
            FileStoreTable table, String columnName, String[] documents) throws Exception {
        buildAndCommitIndexForColumn(table, columnName, documents, BinaryRow.EMPTY_ROW);
    }

    private void buildAndCommitIndexForColumn(
            FileStoreTable table, String columnName, String[] documents, BinaryRow partition)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField textField = table.rowType().getField(columnName);

        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestFullTextGlobalIndexerFactory.IDENTIFIER,
                                textField,
                                options);
        for (int i = 0; i < documents.length; i++) {
            writer.write(documents[i], i);
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
                        partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void buildAndCommitBTreeIndex(FileStoreTable table, String[] documents)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField textField = table.rowType().getField(TEXT_FIELD_NAME);

        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table, BTreeGlobalIndexerFactory.IDENTIFIER, textField, options);
        for (int i = 0; i < documents.length; i++) {
            writer.write(BinaryString.fromString(documents[i]), i);
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
                        BTreeGlobalIndexerFactory.IDENTIFIER,
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

    private List<Integer> readIds(FileStoreTable table, GlobalIndexResult result) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        return ids;
    }

    private void buildAndCommitMultipleIndexFiles(FileStoreTable table, String[] documents)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField textField = table.rowType().getField(TEXT_FIELD_NAME);
        int mid = documents.length / 2;

        // Build first index file covering rows [0, mid)
        GlobalIndexSingleColumnWriter writer1 =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestFullTextGlobalIndexerFactory.IDENTIFIER,
                                textField,
                                options);
        for (int i = 0; i < mid; i++) {
            writer1.write(documents[i], i);
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
        GlobalIndexSingleColumnWriter writer2 =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestFullTextGlobalIndexerFactory.IDENTIFIER,
                                textField,
                                options);
        for (int i = mid; i < documents.length; i++) {
            writer2.write(documents[i], i - mid);
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
