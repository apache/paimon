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

package org.apache.paimon.flink;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexScanner;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for the reverse-btree global index built via {@code create_global_index}. */
public class ReverseBTreeGlobalIndexITCase extends CatalogITCaseBase {

    private static final String[] SUFFIXES = {"red", "green", "blue", "gold"};

    @Test
    public void testReverseBTreeEndsWithEndToEnd() throws Exception {
        int numRows = 4_000;
        sql(
                "CREATE TABLE T_REV (id INT, name STRING) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");

        String values =
                IntStream.range(0, numRows)
                        .mapToObj(i -> String.format("(%d, '%s')", i, name(i)))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO T_REV VALUES " + values);

        sql(
                "CALL sys.create_global_index(`table` => 'default.T_REV', "
                        + "index_column => 'name', index_type => 'reverse-btree', "
                        + "options => 'sorted-index.records-per-range=200')");

        FileStoreTable table = paimonTable("T_REV");
        List<IndexFileMeta> reverseEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(f -> "reverse-btree".equals(f.indexType()))
                        .collect(Collectors.toList());
        assertThat(reverseEntries).isNotEmpty();
        assertThat(reverseEntries.stream().mapToLong(IndexFileMeta::rowCount).sum())
                .isEqualTo(numRows);

        int nameIdx = table.rowType().getFieldIndex("name");
        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());

        Optional<GlobalIndexScanner> scannerOpt =
                GlobalIndexScanner.create(table, new HashSet<>(reverseEntries));
        assertThat(scannerOpt).isPresent();

        Set<Long> unionOfAllSuffixes = new HashSet<>();
        try (GlobalIndexScanner scanner = scannerOpt.get()) {
            for (String suffix : SUFFIXES) {
                Optional<GlobalIndexResult> result =
                        scanner.scan(
                                predicateBuilder.endsWith(
                                        nameIdx, BinaryString.fromString(suffix)));
                assertThat(result).isPresent();

                List<Long> matched = rowIds(result.get());

                assertThat(matched).hasSize(numRows / SUFFIXES.length);
                unionOfAllSuffixes.addAll(matched);
            }

            Optional<GlobalIndexResult> none =
                    scanner.scan(
                            predicateBuilder.endsWith(
                                    nameIdx, BinaryString.fromString("nosuchsuffix")));
            assertThat(none).isPresent();
            assertThat(rowIds(none.get())).isEmpty();
        }

        assertThat(unionOfAllSuffixes).hasSize(numRows);
    }

    private static String name(int i) {
        return "row" + i + SUFFIXES[i % SUFFIXES.length];
    }

    private static List<Long> rowIds(GlobalIndexResult result) {
        List<Long> out = new ArrayList<>();
        result.results().iterator().forEachRemaining(out::add);
        return out;
    }
}
