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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests Java API dispatch for configured primary-key full-text indexes. */
class PrimaryKeyFullTextSearchTest {

    @Test
    void testDispatchesConfiguredFieldToPrimaryKeyScan() {
        FileStoreTable table = table(false);

        FullTextScan scan =
                new FullTextSearchBuilderImpl(table)
                        .withQuery("content", "hello")
                        .withLimit(10)
                        .newFullTextScan();

        assertThat(scan).isInstanceOf(PrimaryKeyFullTextScan.class);
    }

    @Test
    void testRejectsMismatchedPrimaryKeyFullTextField() {
        FileStoreTable table = table(false);

        assertThatThrownBy(
                        () ->
                                new FullTextSearchBuilderImpl(table)
                                        .withQuery("other", "hello")
                                        .withLimit(10)
                                        .newFullTextScan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("other")
                .hasMessageContaining(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS.key());
    }

    @Test
    void testDataEvolutionRetainsGlobalFullTextPath() {
        FileStoreTable table = table(true);

        FullTextScan scan =
                new FullTextSearchBuilderImpl(table)
                        .withQuery("content", "hello")
                        .withLimit(10)
                        .newFullTextScan();

        assertThat(scan).isInstanceOf(FullTextScanImpl.class);
    }

    private static FileStoreTable table(boolean dataEvolution) {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "2");
        options.put(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS.key(), "content");
        options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), Boolean.toString(dataEvolution));
        TableSchema schema =
                new TableSchema(
                        1,
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT().notNull()),
                                new DataField(1, "content", DataTypes.STRING()),
                                new DataField(2, "other", DataTypes.STRING())),
                        2,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        options,
                        null);
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.schema()).thenReturn(schema);
        when(table.rowType()).thenReturn(schema.logicalRowType());
        when(table.coreOptions()).thenReturn(new CoreOptions(options));
        return table;
    }
}
