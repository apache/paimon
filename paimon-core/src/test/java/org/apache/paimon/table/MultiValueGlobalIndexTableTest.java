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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.ScanResult;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexTestUtils;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the multivalue global index on Data Evolution tables. */
public class MultiValueGlobalIndexTableTest extends TableTestBase {

    private static final Integer RED = 1;
    private static final Integer BLUE = 2;

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("tags", DataTypes.ARRAY(DataTypes.INT()))
                .option("bucket", "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.GLOBAL_INDEX_ENABLED.key(), "true")
                .build();
    }

    @Test
    public void testCoreScanUsesMultiValueIndexAndPreservesCoverage() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        write(
                table,
                GenericRow.of(1, array(RED, BLUE)),
                GenericRow.of(2, array(BLUE)),
                GenericRow.of(3, null),
                GenericRow.of(4, array()),
                GenericRow.of(5, array(null, RED, RED)));

        buildIndex(table);
        table = getTableDefault();

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate containsRed = builder.arrayContains(1, RED);
        assertThat(readIds(table, containsRed)).containsExactlyInAnyOrder(1, 5);
        assertThat(readIdsWithFallback(table, builder.isNull(1))).containsExactly(3);

        write(table, GenericRow.of(6, array(RED)));
        table = getTableDefault();

        // Fast search only returns covered rows. Full search retains the uncovered append.
        assertThat(readIds(table, containsRed)).containsExactlyInAnyOrder(1, 5);
        FileStoreTable fullSearchTable =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "full"));
        assertThat(readIds(fullSearchTable, containsRed)).containsExactlyInAnyOrder(1, 5, 6);
    }

    private void buildIndex(FileStoreTable table) throws Exception {
        SortedGlobalIndexScanner scanner =
                new SortedGlobalIndexScanner(table, "multivalue").withIndexField("tags");
        ScanResult<DataSplit> scanResult =
                scanner.incrementalScan()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected data to build multivalue index."));
        List<CommitMessage> messages = new ArrayList<>();
        for (DataSplit split : scanResult.entries()) {
            messages.addAll(
                    SortedGlobalIndexTestUtils.buildIndex(
                            table, "multivalue", "tags", split, scanResult.scanSnapshotId()));
        }
        scanResult
                .deletedIndexEntries()
                .forEach(
                        entry ->
                                messages.add(
                                        new CommitMessageImpl(
                                                entry.partition(),
                                                entry.bucket(),
                                                null,
                                                DataIncrement.deleteIndexIncrement(
                                                        Collections.singletonList(
                                                                entry.indexFile())),
                                                CompactIncrement.emptyIncrement())));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
    }

    private List<Integer> readIds(FileStoreTable table, Predicate predicate) throws Exception {
        return readIds(table, predicate, true);
    }

    private List<Integer> readIdsWithFallback(FileStoreTable table, Predicate predicate)
            throws Exception {
        return readIds(table, predicate, false);
    }

    private List<Integer> readIds(
            FileStoreTable table, Predicate predicate, boolean expectIndexedSplits)
            throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        TableScan.Plan plan = readBuilder.newScan().plan();
        if (expectIndexedSplits) {
            assertThat(plan.splits()).allMatch(IndexedSplit.class::isInstance);
        } else {
            assertThat(plan.splits()).noneMatch(IndexedSplit.class::isInstance);
        }

        List<Integer> ids = new ArrayList<>();
        readBuilder
                .newRead()
                .executeFilter()
                .createReader(plan)
                .forEachRemaining(row -> ids.add(row.getInt(0)));
        return ids;
    }

    private GenericArray array(Object... elements) {
        return new GenericArray(elements);
    }
}
