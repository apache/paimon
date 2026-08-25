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

package org.apache.paimon.table.format;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.mock;

/** Tests for empty Format Table scans selected by an explicit false partition predicate. */
class FormatTableAlwaysFalseScanTest {

    @Test
    void testAlwaysFalseSkipsAllScanBackends() {
        assertAll(
                () -> assertEmptyWithoutBackendAccess(createTable("unpartitioned", false, null)),
                () ->
                        assertEmptyWithoutBackendAccess(
                                createTable("filesystem_partitioned", true, null)),
                () ->
                        assertEmptyWithoutBackendAccess(
                                createTable(
                                        "catalog_managed",
                                        true,
                                        failOnAccess(
                                                FormatTablePartitionManager.class, "catalog"))));
    }

    @Test
    void testSerializedAlwaysFalseSkipsUnpartitionedBackend() throws Exception {
        PartitionPredicate restoredAlwaysFalse =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(PartitionPredicate.ALWAYS_FALSE),
                        getClass().getClassLoader());
        FormatTableScan scan =
                (FormatTableScan)
                        createTable("serialized", false, null)
                                .newReadBuilder()
                                .withPartitionFilter(restoredAlwaysFalse)
                                .newScan();

        assertEmptyWithoutBackendAccess(scan);
    }

    @Test
    void testEquivalentNonSingletonPredicateKeepsNormalScanPath() {
        FormatTable table = createTable("non_singleton", false, null);
        PartitionPredicate equivalentPredicate =
                PartitionPredicate.and(
                        Arrays.asList(
                                PartitionPredicate.ALWAYS_FALSE, PartitionPredicate.ALWAYS_TRUE));
        assertThat(equivalentPredicate).isNotSameAs(PartitionPredicate.ALWAYS_FALSE);
        FormatTableScan scan = new FormatTableScan(table, equivalentPredicate, null);

        assertAll(
                () ->
                        assertThatThrownBy(() -> scan.plan().splits())
                                .isInstanceOf(AssertionError.class)
                                .hasMessageContaining("FileIO accessed"),
                () ->
                        assertThatThrownBy(scan::listPartitionEntries)
                                .isInstanceOf(AssertionError.class)
                                .hasMessageContaining("FileIO accessed"));
    }

    private void assertEmptyWithoutBackendAccess(FormatTable table) {
        assertEmptyWithoutBackendAccess(
                new FormatTableScan(table, PartitionPredicate.ALWAYS_FALSE, null));
    }

    private void assertEmptyWithoutBackendAccess(FormatTableScan scan) {
        FormatTableScan.Plan plan = scan.plan();

        assertAll(
                () -> assertThat(plan.splits()).isEmpty(),
                () -> assertThat(plan.rowCount()).isEqualTo(OptionalLong.of(0L)),
                () -> assertThat(scan.listPartitionEntries()).isEmpty());
    }

    private FormatTable createTable(
            String name,
            boolean partitioned,
            @Nullable FormatTablePartitionManager partitionManager) {
        RowType rowType =
                RowType.builder()
                        .field("partition", DataTypes.INT())
                        .field("value", DataTypes.INT())
                        .build();
        List<String> partitionKeys =
                partitioned ? Collections.singletonList("partition") : Collections.emptyList();
        return FormatTable.builder()
                .fileIO(failOnAccess(FileIO.class, "FileIO"))
                .identifier(Identifier.create("test_db", name))
                .rowType(rowType)
                .partitionKeys(partitionKeys)
                .location("file:/path/which/must/not/be/accessed")
                .format(FormatTable.Format.CSV)
                .options(Collections.emptyMap())
                .partitionManager(partitionManager)
                .build();
    }

    private static <T> T failOnAccess(Class<T> backendClass, String backendName) {
        return mock(
                backendClass,
                invocation -> {
                    throw new AssertionError(
                            backendName + " accessed through " + invocation.getMethod().getName());
                });
    }
}
