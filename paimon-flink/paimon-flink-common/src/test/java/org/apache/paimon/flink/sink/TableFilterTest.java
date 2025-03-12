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

package org.apache.paimon.flink.sink;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for TableFilter. */
public class TableFilterTest {

    @Test
    public void testFilterTables() {
        List<String> tableWhitelist = Collections.singletonList("white_listed_table");
        List<String> tablePrefixes = Collections.singletonList("test_");
        List<String> tableSuffixes = Collections.EMPTY_LIST;
        String tblIncludingPattern = ".*";
        String tblExcludingPattern = "";

        // prefix, no suffix, no whitelisting,
        List<String> allTables =
                Arrays.asList(
                        "test_table1",
                        "test_table2",
                        "test_table1_suffix_in",
                        "test_table2_suffix_in",
                        "test_table1_suffix_ex",
                        "test_table2_suffix_ex",
                        "test_excluded1",
                        "test_excluded2",
                        "white_listed_table",
                        "other_table1",
                        "other_table2");

        TableFilter tableFilter =
                new TableFilter(
                        "",
                        tableWhitelist,
                        tablePrefixes,
                        tableSuffixes,
                        tblIncludingPattern,
                        tblExcludingPattern);

        List<String> filteredTables = tableFilter.filterTables(allTables);
        assertThat(filteredTables.size()).isEqualTo(9);
        assertThat(filteredTables)
                .contains(
                        "test_table1",
                        "test_table2",
                        "test_table1_suffix_in",
                        "test_table2_suffix_in",
                        "test_table1_suffix_ex",
                        "test_table2_suffix_ex",
                        "test_excluded1",
                        "test_excluded2",
                        "white_listed_table");

        // exclude pattern
        tblExcludingPattern = "excluded.*";
        tableFilter.setTblExcludingPattern(tblExcludingPattern);
        filteredTables = tableFilter.filterTables(allTables);
        assertThat(filteredTables.size()).isEqualTo(7);
        assertThat(filteredTables)
                .contains(
                        "test_table1",
                        "test_table2",
                        "test_table1_suffix_in",
                        "test_table2_suffix_in",
                        "test_table1_suffix_ex",
                        "test_table2_suffix_ex",
                        "white_listed_table");

        // suffix
        tableSuffixes = Collections.singletonList("_suffix_in");
        tableFilter.setTableSuffixes(tableSuffixes);
        filteredTables = tableFilter.filterTables(allTables);
        assertThat(filteredTables.size()).isEqualTo(3);
        assertThat(filteredTables)
                .contains("test_table1_suffix_in", "test_table2_suffix_in", "white_listed_table");

        // No tables
        filteredTables = tableFilter.filterTables(Collections.emptyList());
        assertThat(filteredTables.size()).isEqualTo(0);
    }
}
