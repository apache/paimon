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
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexResult;
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

    private static final String[] NON_ASCII_SUFFIXES = {"data", "café", "grün", "中文"};

    @Test
    public void testReverseBTreeEndsWithEndToEnd() throws Exception {
        runEndsWithEndToEnd("T_REV", SUFFIXES);
    }

    @Test
    public void testReverseBTreeEndsWithNonAsciiEndToEnd() throws Exception {
        runEndsWithEndToEnd("T_REV_UNI", NON_ASCII_SUFFIXES);
    }

    private void runEndsWithEndToEnd(String tableName, String[] suffixes) throws Exception {
        int numRows = 4_000;
        sql(
                "CREATE TABLE %s (id INT, name STRING) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")",
                tableName);

        List<String> names =
                IntStream.range(0, numRows)
                        .mapToObj(i -> "row" + i + suffixes[i % suffixes.length])
                        .collect(Collectors.toList());
        String values =
                IntStream.range(0, numRows)
                        .mapToObj(i -> String.format("(%d, '%s')", i, names.get(i)))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO " + tableName + " VALUES " + values);

        sql(
                "CALL sys.create_global_index(`table` => 'default.%s', "
                        + "index_column => 'name', index_type => 'reverse-btree', "
                        + "options => 'sorted-index.records-per-range=200')",
                tableName);

        FileStoreTable table = paimonTable(tableName);
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

        Optional<DataEvolutionGlobalIndexScanner> scannerOpt =
                DataEvolutionGlobalIndexScanner.create(table, new HashSet<>(reverseEntries));
        assertThat(scannerOpt).isPresent();

        Set<Long> unionOfAllSuffixes = new HashSet<>();
        try (DataEvolutionGlobalIndexScanner scanner = scannerOpt.get()) {
            for (String suffix : suffixes) {
                List<Long> matched = scanRowIds(scanner, predicateBuilder, nameIdx, suffix);
                assertThat(matched).hasSize(countEndingWith(names, suffix));
                unionOfAllSuffixes.addAll(matched);
            }

            // a one-character suffix spans several full suffixes ("d" hits both "red" and
            // "gold") and, for multi-byte characters, prefix-scans on a partial code point
            for (String suffix : suffixes) {
                String lastChar = suffix.substring(suffix.length() - 1);
                assertThat(scanRowIds(scanner, predicateBuilder, nameIdx, lastChar))
                        .hasSize(countEndingWith(names, lastChar));
            }

            assertThat(scanRowIds(scanner, predicateBuilder, nameIdx, "nosuchsuffix")).isEmpty();
        }

        assertThat(unionOfAllSuffixes).hasSize(numRows);
    }

    private static List<Long> scanRowIds(
            DataEvolutionGlobalIndexScanner scanner,
            PredicateBuilder predicateBuilder,
            int nameIdx,
            String suffix) {
        Optional<GlobalIndexResult> result =
                scanner.scan(predicateBuilder.endsWith(nameIdx, BinaryString.fromString(suffix)));
        assertThat(result).isPresent();
        return rowIds(result.get());
    }

    private static int countEndingWith(List<String> names, String suffix) {
        return (int) names.stream().filter(name -> name.endsWith(suffix)).count();
    }

    private static List<Long> rowIds(GlobalIndexResult result) {
        List<Long> out = new ArrayList<>();
        result.results().iterator().forEachRemaining(out::add);
        return out;
    }
}
