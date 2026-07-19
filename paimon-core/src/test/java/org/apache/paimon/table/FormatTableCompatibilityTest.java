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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.format.FormatTableCatalogProvider;
import org.apache.paimon.table.format.FormatTableCommit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
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
    void testCopyRejectsManagedPartitionStateChange(boolean managed) {
        FormatTable table = formatTable(Boolean.toString(managed));

        assertThatThrownBy(
                        () ->
                                table.copy(
                                        Collections.singletonMap(
                                                METASTORE_PARTITIONED_TABLE.key(),
                                                Boolean.toString(!managed))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(METASTORE_PARTITIONED_TABLE.key());
    }

    @Test
    void testCopyAllowsSemanticallyEquivalentManagedPartitionOption() {
        FormatTable table = formatTable("TRUE");

        FormatTable copied =
                table.copy(Collections.singletonMap(METASTORE_PARTITIONED_TABLE.key(), "true"));

        assertThat(copied).isNotSameAs(table);
        assertThat(copied.options())
                .containsEntry(METASTORE_PARTITIONED_TABLE.key(), Boolean.TRUE.toString());
    }

    @Test
    void testCopyRejectsChangingAbsentManagedPartitionDefault() {
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
    void testManagedCopyRejectsPhysicalPartitionLayoutChange(boolean onlyValueInPath) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(METASTORE_PARTITIONED_TABLE.key(), "true");
        options.put(
                FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key(), Boolean.toString(onlyValueInPath));
        FormatTable table = formatTable(options, null);

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
    void testCopyPreservesManagedStateAndCatalogProviderForUnrelatedOption(boolean managed) {
        Identifier identifier = Identifier.create("managed_db", "managed_table");
        FormatTableCatalogProvider catalogProvider =
                new FormatTableCatalogProvider(identifier, () -> null);
        Map<String, String> options = new LinkedHashMap<>();
        options.put(METASTORE_PARTITIONED_TABLE.key(), Boolean.toString(managed));
        FormatTable table = formatTable(options, catalogProvider);

        FormatTable copied = table.copy(Collections.singletonMap(READ_BATCH_SIZE.key(), "128"));

        assertThat(copied).isNotSameAs(table);
        assertThat(copied.options())
                .containsEntry(READ_BATCH_SIZE.key(), "128")
                .containsEntry(METASTORE_PARTITIONED_TABLE.key(), Boolean.toString(managed));
        assertThat(
                        new org.apache.paimon.CoreOptions(copied.options())
                                .partitionedTableInMetastore())
                .isEqualTo(managed);
        assertThat(copied.catalogProvider()).isSameAs(catalogProvider);
        assertThat(table.options())
                .containsExactlyEntriesOf(options)
                .doesNotContainKey(READ_BATCH_SIZE.key());
    }

    @Test
    void testManagedCatalogExtensionKeepsExistingImplementationsCompatible() throws Exception {
        assertThat(FormatTable.class.getMethod("catalogProvider").isDefault()).isTrue();
        assertThat(
                        FormatTable.FormatTableImpl.class.getConstructor(
                                FileIO.class,
                                Identifier.class,
                                RowType.class,
                                List.class,
                                String.class,
                                FormatTable.Format.class,
                                Map.class,
                                String.class,
                                CatalogContext.class))
                .isNotNull();
        assertThat(
                        FormatTableCommit.class.getConstructor(
                                String.class,
                                List.class,
                                FileIO.class,
                                boolean.class,
                                boolean.class,
                                Identifier.class,
                                Map.class,
                                String.class,
                                CatalogContext.class))
                .isNotNull();
    }

    private static FormatTable formatTable(String managed) {
        return formatTable(
                Collections.singletonMap(METASTORE_PARTITIONED_TABLE.key(), managed), null);
    }

    private static FormatTable formatTable(
            Map<String, String> options, FormatTableCatalogProvider catalogProvider) {
        return FormatTable.builder()
                .fileIO(LocalFileIO.create())
                .identifier(Identifier.create("managed_db", "managed_table"))
                .rowType(
                        RowType.builder()
                                .field("id", DataTypes.INT())
                                .field("dt", DataTypes.STRING())
                                .build())
                .partitionKeys(Collections.singletonList("dt"))
                .location("file:///warehouse/managed_table")
                .format(FormatTable.Format.PARQUET)
                .options(options)
                .catalogProvider(catalogProvider)
                .build();
    }
}
