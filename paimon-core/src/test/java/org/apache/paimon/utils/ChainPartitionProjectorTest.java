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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link ChainPartitionProjector}. */
public class ChainPartitionProjectorTest {

    // ========================== Helper Methods ==========================

    /** Build a BinaryRow with given string values for a STRING-only RowType. */
    private static BinaryRow buildStringRow(String... values) {
        BinaryRow row = new BinaryRow(values.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                writer.setNullAt(i);
            } else {
                writer.writeString(i, BinaryString.fromString(values[i]));
            }
        }
        writer.complete();
        return row;
    }

    /** Build a BinaryRow with mixed types: string at pos 0, int at pos 1. */
    private static BinaryRow buildStringIntRow(String strVal, int intVal) {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryString.fromString(strVal));
        writer.writeInt(1, intVal);
        writer.complete();
        return row;
    }

    /** Extract a string value from a BinaryRow at the given position. */
    private static String getString(BinaryRow row, int pos) {
        return row.getString(pos).toString();
    }

    // ========================== Test: Two STRING fields (region, date) ==========================

    @Test
    public void testExtractGroupAndChainWithTwoStringFields() {
        // partition keys: (region, date), chain partition keys: (date)
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        BinaryRow fullPartition = buildStringRow("US", "20250726");

        // extract group
        BinaryRow groupPart = projector.extractGroupPartition(fullPartition);
        assertThat(groupPart.getFieldCount()).isEqualTo(1);
        assertThat(getString(groupPart, 0)).isEqualTo("US");

        // extract chain
        BinaryRow chainPart = projector.extractChainPartition(fullPartition);
        assertThat(chainPart.getFieldCount()).isEqualTo(1);
        assertThat(getString(chainPart, 0)).isEqualTo("20250726");
    }

    @Test
    public void testCombinePartitionWithTwoStringFields() {
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        BinaryRow groupPart = buildStringRow("EU");
        BinaryRow chainPart = buildStringRow("20250801");

        BinaryRow combined = projector.combinePartition(groupPart, chainPart);
        assertThat(combined.getFieldCount()).isEqualTo(2);
        assertThat(getString(combined, 0)).isEqualTo("EU");
        assertThat(getString(combined, 1)).isEqualTo("20250801");
    }

    @Test
    public void testRoundTripExtractThenCombineWithTwoStringFields() {
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        BinaryRow original = buildStringRow("CN", "20250720");

        BinaryRow groupPart = projector.extractGroupPartition(original);
        BinaryRow chainPart = projector.extractChainPartition(original);
        BinaryRow reconstructed = projector.combinePartition(groupPart, chainPart);

        assertThat(getString(reconstructed, 0)).isEqualTo("CN");
        assertThat(getString(reconstructed, 1)).isEqualTo("20250720");

        // verify binary equality via serialization
        InternalRowSerializer serializer = new InternalRowSerializer(fullType);
        assertThat(serializer.toBinaryRow(reconstructed).copy())
                .isEqualTo(serializer.toBinaryRow(original).copy());
    }

    // ========================== Test: Three STRING fields ==========================

    @Test
    public void testThreeStringFieldsWithTwoChain() {
        // partition keys: (country, city, date), chain partition keys: (city, date)
        RowType fullType =
                RowType.builder()
                        .field("country", DataTypes.STRING().notNull())
                        .field("city", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 2);

        assertThat(projector.groupFieldCount()).isEqualTo(1);
        assertThat(projector.chainFieldCount()).isEqualTo(2);
        assertThat(projector.hasGroupPartition()).isTrue();

        BinaryRow fullPartition = buildStringRow("China", "Beijing", "20250801");

        // group = (country)
        BinaryRow groupPart = projector.extractGroupPartition(fullPartition);
        assertThat(groupPart.getFieldCount()).isEqualTo(1);
        assertThat(getString(groupPart, 0)).isEqualTo("China");

        // chain = (city, date)
        BinaryRow chainPart = projector.extractChainPartition(fullPartition);
        assertThat(chainPart.getFieldCount()).isEqualTo(2);
        assertThat(getString(chainPart, 0)).isEqualTo("Beijing");
        assertThat(getString(chainPart, 1)).isEqualTo("20250801");

        // round-trip
        BinaryRow combined = projector.combinePartition(groupPart, chainPart);
        assertThat(getString(combined, 0)).isEqualTo("China");
        assertThat(getString(combined, 1)).isEqualTo("Beijing");
        assertThat(getString(combined, 2)).isEqualTo("20250801");
    }

    @Test
    public void testThreeStringFieldsWithOneChain() {
        // partition keys: (country, city, date), chain partition keys: (date)
        RowType fullType =
                RowType.builder()
                        .field("country", DataTypes.STRING().notNull())
                        .field("city", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        assertThat(projector.groupFieldCount()).isEqualTo(2);
        assertThat(projector.chainFieldCount()).isEqualTo(1);

        BinaryRow fullPartition = buildStringRow("US", "NYC", "20250901");

        // group = (country, city)
        BinaryRow groupPart = projector.extractGroupPartition(fullPartition);
        assertThat(groupPart.getFieldCount()).isEqualTo(2);
        assertThat(getString(groupPart, 0)).isEqualTo("US");
        assertThat(getString(groupPart, 1)).isEqualTo("NYC");

        // chain = (date)
        BinaryRow chainPart = projector.extractChainPartition(fullPartition);
        assertThat(chainPart.getFieldCount()).isEqualTo(1);
        assertThat(getString(chainPart, 0)).isEqualTo("20250901");
    }

    // ========================== Test: All partitions are chain (no group)
    // ==========================

    @Test
    public void testAllChainWithNoGroup() {
        // partition keys: (date), chain partition keys: (date)
        RowType fullType = RowType.builder().field("date", DataTypes.STRING().notNull()).build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        assertThat(projector.groupFieldCount()).isEqualTo(0);
        assertThat(projector.chainFieldCount()).isEqualTo(1);
        assertThat(projector.hasGroupPartition()).isFalse();

        BinaryRow fullPartition = buildStringRow("20250726");

        // group is empty
        BinaryRow groupPart = projector.extractGroupPartition(fullPartition);
        assertThat(groupPart.getFieldCount()).isEqualTo(0);

        // chain is the whole partition
        BinaryRow chainPart = projector.extractChainPartition(fullPartition);
        assertThat(chainPart.getFieldCount()).isEqualTo(1);
        assertThat(getString(chainPart, 0)).isEqualTo("20250726");
    }

    @Test
    public void testAllChainWithTwoFields() {
        // partition keys: (dt, hour), chain partition keys: (dt, hour)
        RowType fullType =
                RowType.builder()
                        .field("dt", DataTypes.STRING().notNull())
                        .field("hour", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 2);

        assertThat(projector.hasGroupPartition()).isFalse();
        assertThat(projector.groupFieldCount()).isEqualTo(0);
        assertThat(projector.chainFieldCount()).isEqualTo(2);

        BinaryRow fullPartition = buildStringRow("20250810", "14");

        BinaryRow chainPart = projector.extractChainPartition(fullPartition);
        assertThat(getString(chainPart, 0)).isEqualTo("20250810");
        assertThat(getString(chainPart, 1)).isEqualTo("14");
    }

    // ========================== Test: Mixed types (STRING + INT) ==========================

    @Test
    public void testMixedTypesWithStringAndInt() {
        // partition keys: (region STRING, bucket_id INT), chain partition keys: (bucket_id)
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("bucket_id", DataTypes.INT().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        BinaryRow fullPartition = buildStringIntRow("US", 42);

        // group = (region)
        BinaryRow groupPart = projector.extractGroupPartition(fullPartition);
        assertThat(groupPart.getFieldCount()).isEqualTo(1);
        assertThat(getString(groupPart, 0)).isEqualTo("US");

        // chain = (bucket_id)
        BinaryRow chainPart = projector.extractChainPartition(fullPartition);
        assertThat(chainPart.getFieldCount()).isEqualTo(1);
        assertThat(chainPart.getInt(0)).isEqualTo(42);

        // combine
        BinaryRow combined = projector.combinePartition(groupPart, chainPart);
        assertThat(getString(combined, 0)).isEqualTo("US");
        assertThat(combined.getInt(1)).isEqualTo(42);
    }

    // ========================== Test: RowType accessors ==========================

    @Test
    public void testGroupPartitionType() {
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("city", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        RowType groupType = projector.groupPartitionType();
        assertThat(groupType.getFieldCount()).isEqualTo(2);
        assertThat(groupType.getFieldNames().get(0)).isEqualTo("region");
        assertThat(groupType.getFieldNames().get(1)).isEqualTo("city");

        RowType chainType = projector.chainPartitionType();
        assertThat(chainType.getFieldCount()).isEqualTo(1);
        assertThat(chainType.getFieldNames().get(0)).isEqualTo("date");
    }

    @Test
    public void testGroupPartitionTypeWithNoGroup() {
        RowType fullType = RowType.builder().field("date", DataTypes.STRING().notNull()).build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        RowType groupType = projector.groupPartitionType();
        assertThat(groupType.getFieldCount()).isEqualTo(0);

        RowType chainType = projector.chainPartitionType();
        assertThat(chainType.getFieldCount()).isEqualTo(1);
        assertThat(chainType.getFieldNames().get(0)).isEqualTo("date");
    }

    // ========================== Test: Multiple partitions grouping ==========================

    @Test
    public void testGroupPartitionConsistency() {
        // Verify that partitions with same group prefix produce equal group projections
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        BinaryRow partition1 = buildStringRow("US", "20250721");
        BinaryRow partition2 = buildStringRow("US", "20250722");
        BinaryRow partition3 = buildStringRow("EU", "20250721");

        BinaryRow group1 = projector.extractGroupPartition(partition1).copy();
        BinaryRow group2 = projector.extractGroupPartition(partition2).copy();
        BinaryRow group3 = projector.extractGroupPartition(partition3).copy();

        // Same region => equal group partitions
        assertThat(group1).isEqualTo(group2);

        // Different region => different group partitions
        assertThat(group1).isNotEqualTo(group3);
    }

    @Test
    public void testChainPartitionOrdering() {
        // Verify that chain projections can be compared correctly
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        BinaryRow partition1 = buildStringRow("US", "20250720");
        BinaryRow partition2 = buildStringRow("US", "20250726");
        BinaryRow partition3 = buildStringRow("EU", "20250720");

        BinaryRow chain1 = projector.extractChainPartition(partition1).copy();
        BinaryRow chain2 = projector.extractChainPartition(partition2).copy();
        BinaryRow chain3 = projector.extractChainPartition(partition3).copy();

        // Same date, different region => equal chain partitions
        assertThat(chain1).isEqualTo(chain3);

        // Different date => different chain partitions
        assertThat(chain1).isNotEqualTo(chain2);

        // Verify string ordering: "20250720" < "20250726"
        assertThat(chain1.getString(0).compareTo(chain2.getString(0))).isLessThan(0);
    }

    // ========================== Test: Nullable fields ==========================

    @Test
    public void testNullableGroupField() {
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        // build a row with null region
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.setNullAt(0);
        writer.writeString(1, BinaryString.fromString("20250801"));
        writer.complete();

        BinaryRow groupPart = projector.extractGroupPartition(row);
        assertThat(groupPart.isNullAt(0)).isTrue();

        BinaryRow chainPart = projector.extractChainPartition(row);
        assertThat(getString(chainPart, 0)).isEqualTo("20250801");

        // combine should preserve null
        BinaryRow combined = projector.combinePartition(groupPart, chainPart);
        assertThat(combined.isNullAt(0)).isTrue();
        assertThat(getString(combined, 1)).isEqualTo("20250801");
    }

    // ========================== Test: Combine with different group/chain sources

    @Test
    public void testCombineFromDifferentSources() {
        // Simulate: take group from one partition, chain from another
        RowType fullType =
                RowType.builder()
                        .field("region", DataTypes.STRING().notNull())
                        .field("date", DataTypes.STRING().notNull())
                        .build();

        ChainPartitionProjector projector = new ChainPartitionProjector(fullType, 1);

        BinaryRow partitionA = buildStringRow("US", "20250720");
        BinaryRow partitionB = buildStringRow("EU", "20250801");

        // Take group from A (US), chain from B (20250801)
        BinaryRow groupFromA = projector.extractGroupPartition(partitionA);
        BinaryRow chainFromB = projector.extractChainPartition(partitionB);

        BinaryRow mixed = projector.combinePartition(groupFromA, chainFromB);
        assertThat(getString(mixed, 0)).isEqualTo("US");
        assertThat(getString(mixed, 1)).isEqualTo("20250801");
    }
}
