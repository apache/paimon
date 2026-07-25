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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.format.FormatTablePartitionManager;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.CoreOptions.FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH;
import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.CoreOptions.READ_BATCH_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Compatibility tests for the public {@link FormatTable} API. */
class FormatTableCompatibilityTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCopyRejectsCatalogManagedPartitionStateChange(boolean withCatalogManagedPartitions) {
        FormatTable table = formatTable(Boolean.toString(withCatalogManagedPartitions));

        assertThatThrownBy(
                        () ->
                                table.copy(
                                        Collections.singletonMap(
                                                METASTORE_PARTITIONED_TABLE.key(),
                                                Boolean.toString(!withCatalogManagedPartitions))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(METASTORE_PARTITIONED_TABLE.key());
    }

    @Test
    void testCopyAllowsSemanticallyEquivalentCatalogManagedPartitionOption() {
        FormatTable table = formatTable("true");

        FormatTable copied =
                table.copy(Collections.singletonMap(METASTORE_PARTITIONED_TABLE.key(), "true"));

        assertThat(copied).isNotSameAs(table);
        assertThat(copied.options()).containsEntry(METASTORE_PARTITIONED_TABLE.key(), "true");
    }

    @Test
    void testCopyRejectsChangingAbsentCatalogManagedPartitionDefault() {
        FormatTable table = formatTable(Collections.emptyMap(), null);

        assertThat(table.options()).doesNotContainKey(METASTORE_PARTITIONED_TABLE.key());
        assertThatThrownBy(
                        () ->
                                table.copy(
                                        Collections.singletonMap(
                                                METASTORE_PARTITIONED_TABLE.key(), "true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(METASTORE_PARTITIONED_TABLE.key());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCatalogManagedPartitionCopyRejectsPhysicalPartitionLayoutChange(
            boolean onlyValueInPath) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(METASTORE_PARTITIONED_TABLE.key(), "true");
        options.put(
                FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key(), Boolean.toString(onlyValueInPath));
        // The layout is only fixed for a table that actually has catalog-managed partitions.
        FormatTable table =
                formatTable(
                        options,
                        FormatTablePartitionManager.create(
                                Identifier.create(
                                        "catalog_partition_db", "catalog_partition_table"),
                                Collections.singletonList("dt"),
                                () -> null));

        assertThatThrownBy(
                        () ->
                                table.copy(
                                        Collections.singletonMap(
                                                FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key(),
                                                Boolean.toString(!onlyValueInPath))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCopyPreservesPartitionModeAndCatalogProviderForUnrelatedOption(
            boolean withCatalogManagedPartitions) {
        Identifier identifier =
                Identifier.create("catalog_partition_db", "catalog_partition_table");
        FormatTablePartitionManager partitionManager =
                FormatTablePartitionManager.create(
                        identifier, Collections.singletonList("dt"), () -> null);
        Map<String, String> options = new LinkedHashMap<>();
        options.put(
                METASTORE_PARTITIONED_TABLE.key(), Boolean.toString(withCatalogManagedPartitions));
        FormatTable table = formatTable(options, partitionManager);

        FormatTable copied = table.copy(Collections.singletonMap(READ_BATCH_SIZE.key(), "128"));

        assertThat(copied).isNotSameAs(table);
        assertThat(copied.options())
                .containsEntry(READ_BATCH_SIZE.key(), "128")
                .containsEntry(
                        METASTORE_PARTITIONED_TABLE.key(),
                        Boolean.toString(withCatalogManagedPartitions));
        assertThat(
                        new org.apache.paimon.CoreOptions(copied.options())
                                .partitionedTableInMetastore())
                .isEqualTo(withCatalogManagedPartitions);
        assertThat(copied.partitionManager()).isSameAs(partitionManager);
        assertThat(table.options())
                .containsExactlyEntriesOf(options)
                .doesNotContainKey(READ_BATCH_SIZE.key());
    }

    @Test
    void testPartitionManagerIsOptionalForOtherImplementations() throws Exception {
        // Implementations outside this repository do not have to know about catalog-managed
        // partitions.
        assertThat(FormatTable.class.getMethod("partitionManager").isDefault()).isTrue();
    }

    private static FormatTable formatTable(String partitionedInMetastore) {
        return formatTable(
                Collections.singletonMap(METASTORE_PARTITIONED_TABLE.key(), partitionedInMetastore),
                null);
    }

    private static FormatTable formatTable(
            Map<String, String> options, FormatTablePartitionManager partitionManager) {
        return FormatTable.builder()
                .fileIO(LocalFileIO.create())
                .identifier(Identifier.create("catalog_partition_db", "catalog_partition_table"))
                .rowType(
                        RowType.builder()
                                .field("id", DataTypes.INT())
                                .field("dt", DataTypes.STRING())
                                .build())
                .partitionKeys(Collections.singletonList("dt"))
                .location("file:///warehouse/catalog_partition_table")
                .format(FormatTable.Format.PARQUET)
                .options(options)
                .partitionManager(partitionManager)
                .build();
    }
}
