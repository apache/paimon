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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.globalindex.sorted.SortedIndexOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CreateGlobalIndexProcedure}. */
public class CreateGlobalIndexProcedureTest {

    @Test
    public void testCreateUserOptionsUsesTableOptionsAndParsedOptionsOverride() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(BTreeIndexOptions.BTREE_INDEX_COMPRESSION.key(), "zstd");
        tableOptions.put(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE.key(), "100");
        tableOptions.put("unrelated-table-option", "table-value");

        Options userOptions =
                CreateGlobalIndexProcedure.createUserOptions(
                        tableOptions,
                        SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE.key()
                                + "=200;procedure-only=procedure-value");

        assertThat(userOptions.get(BTreeIndexOptions.BTREE_INDEX_COMPRESSION)).isEqualTo("zstd");
        assertThat(userOptions.get(SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE))
                .isEqualTo(200L);
        assertThat(userOptions.get("unrelated-table-option")).isEqualTo("table-value");
        assertThat(userOptions.get("procedure-only")).isEqualTo("procedure-value");
    }
}
