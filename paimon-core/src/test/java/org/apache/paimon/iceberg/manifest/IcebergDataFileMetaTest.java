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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryArrayWriter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergDataFileMetaTest {

    @Test
    @DisplayName("Test partition is a required field")
    void testPartitionIsNotNull() {
        RowType partitionType = TestKeyValueGenerator.SINGLE_PARTITIONED_PART_TYPE;
        RowType schema = IcebergDataFileMeta.schema(partitionType);
        List<DataField> fields = schema.getFields();

        Optional<DataField> partitionField = fields.stream().filter(f -> f.id() == 102).findFirst();

        assertThat(partitionField).isPresent();
        assertThat(partitionField.get().name()).isEqualTo("partition");
        assertThat(partitionField.get().type()).isEqualTo(partitionType.notNull());
    }

    @Test
    @DisplayName("Test unknown null count is omitted instead of published as 0")
    void testUnknownNullCountOmitted() {
        IcebergSchema icebergSchema =
                new IcebergSchema(
                        0,
                        Arrays.asList(
                                new IcebergDataField(1, "a", false, "int", null),
                                new IcebergDataField(2, "b", false, "int", null)));

        BinaryRow values = new BinaryRow(2);
        BinaryRowWriter rowWriter = new BinaryRowWriter(values);
        rowWriter.setNullAt(0);
        rowWriter.setNullAt(1);
        rowWriter.complete();

        // "a" has an unknown null count, "b" has a known null count of 0
        BinaryArray nullCounts = new BinaryArray();
        BinaryArrayWriter arrayWriter = new BinaryArrayWriter(nullCounts, 2, 8);
        arrayWriter.setNullLong(0);
        arrayWriter.writeLong(1, 0L);
        arrayWriter.complete();

        IcebergDataFileMeta meta =
                IcebergDataFileMeta.create(
                        IcebergDataFileMeta.Content.DATA,
                        "path",
                        "parquet",
                        BinaryRow.EMPTY_ROW,
                        10,
                        100,
                        icebergSchema,
                        new SimpleStats(values, values, nullCounts),
                        null);

        assertThat(meta.nullValueCounts().size()).isEqualTo(1);
        assertThat(((GenericMap) meta.nullValueCounts()).get(1)).isNull();
        assertThat(((GenericMap) meta.nullValueCounts()).get(2)).isEqualTo(0L);
    }

    @Test
    @DisplayName("Test required field with unknown stats does not publish garbage bounds")
    void testRequiredFieldWithUnknownStats() {
        IcebergSchema icebergSchema =
                new IcebergSchema(
                        0,
                        Arrays.asList(
                                new IcebergDataField(1, "id", true, "int", null),
                                new IcebergDataField(2, "cnt", true, "long", null)));

        BinaryRow values = new BinaryRow(2);
        BinaryRowWriter rowWriter = new BinaryRowWriter(values);
        rowWriter.writeInt(0, 1);
        rowWriter.setNullAt(1);
        rowWriter.complete();

        BinaryArray nullCounts = new BinaryArray();
        BinaryArrayWriter arrayWriter = new BinaryArrayWriter(nullCounts, 2, 8);
        arrayWriter.writeLong(0, 0L);
        arrayWriter.setNullLong(1);
        arrayWriter.complete();

        IcebergDataFileMeta meta =
                IcebergDataFileMeta.create(
                        IcebergDataFileMeta.Content.DATA,
                        "path",
                        "parquet",
                        BinaryRow.EMPTY_ROW,
                        10,
                        100,
                        icebergSchema,
                        new SimpleStats(values, values, nullCounts),
                        null);

        assertThat(meta.nullValueCounts().size()).isEqualTo(1);
        assertThat(((GenericMap) meta.nullValueCounts()).get(1)).isEqualTo(0L);
        assertThat(meta.lowerBounds().size()).isEqualTo(1);
        assertThat(meta.upperBounds().size()).isEqualTo(1);
    }

    @Test
    @DisplayName("Test stats of a statsColumns subset are read by stats index, not field ordinal")
    void testStatsColumnsSubsetAlignment() {
        IcebergSchema icebergSchema =
                new IcebergSchema(
                        0,
                        Arrays.asList(
                                new IcebergDataField(1, "a", false, "int", null),
                                new IcebergDataField(2, "b", false, "long", null)));

        // stats only cover "b": slot 0 in the stats, ordinal 1 in the schema
        BinaryRow values = new BinaryRow(1);
        BinaryRowWriter rowWriter = new BinaryRowWriter(values);
        rowWriter.writeLong(0, 7L);
        rowWriter.complete();

        BinaryArray nullCounts = new BinaryArray();
        BinaryArrayWriter arrayWriter = new BinaryArrayWriter(nullCounts, 1, 8);
        arrayWriter.writeLong(0, 2L);
        arrayWriter.complete();

        IcebergDataFileMeta meta =
                IcebergDataFileMeta.create(
                        IcebergDataFileMeta.Content.DATA,
                        "path",
                        "parquet",
                        BinaryRow.EMPTY_ROW,
                        10,
                        100,
                        icebergSchema,
                        new SimpleStats(values, values, nullCounts),
                        Arrays.asList("b"));

        assertThat(meta.nullValueCounts().size()).isEqualTo(1);
        assertThat(((GenericMap) meta.nullValueCounts()).get(2)).isEqualTo(2L);
        assertThat(meta.lowerBounds().size()).isEqualTo(1);
        byte[] expectedLong7 = {7, 0, 0, 0, 0, 0, 0, 0};
        assertThat((byte[]) ((GenericMap) meta.lowerBounds()).get(2)).isEqualTo(expectedLong7);
        assertThat((byte[]) ((GenericMap) meta.upperBounds()).get(2)).isEqualTo(expectedLong7);
    }

    @Test
    @DisplayName("Test required nested field with unknown stats is skipped before reading slots")
    void testRequiredNestedFieldSkipped() {
        IcebergSchema icebergSchema =
                new IcebergSchema(
                        0,
                        Arrays.asList(
                                new IcebergDataField(1, "id", true, "int", null),
                                new IcebergDataField(
                                        new DataField(
                                                2,
                                                "arr",
                                                DataTypes.ARRAY(DataTypes.INT()).notNull()))));

        BinaryRow minValues = new BinaryRow(2);
        BinaryRowWriter minWriter = new BinaryRowWriter(minValues);
        minWriter.writeInt(0, 1);
        minWriter.setNullAt(1);
        minWriter.complete();

        BinaryRow maxValues = new BinaryRow(2);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxValues);
        maxWriter.writeInt(0, 5);
        maxWriter.setNullAt(1);
        maxWriter.complete();

        BinaryArray nullCounts = new BinaryArray();
        BinaryArrayWriter arrayWriter = new BinaryArrayWriter(nullCounts, 2, 8);
        arrayWriter.writeLong(0, 0L);
        arrayWriter.setNullLong(1);
        arrayWriter.complete();

        IcebergDataFileMeta meta =
                IcebergDataFileMeta.create(
                        IcebergDataFileMeta.Content.DATA,
                        "path",
                        "parquet",
                        BinaryRow.EMPTY_ROW,
                        10,
                        100,
                        icebergSchema,
                        new SimpleStats(minValues, maxValues, nullCounts),
                        null);

        assertThat(meta.lowerBounds().size()).isEqualTo(1);
        assertThat(meta.upperBounds().size()).isEqualTo(1);
        assertThat((byte[]) ((GenericMap) meta.lowerBounds()).get(1))
                .isEqualTo(new byte[] {1, 0, 0, 0});
        assertThat((byte[]) ((GenericMap) meta.upperBounds()).get(1))
                .isEqualTo(new byte[] {5, 0, 0, 0});
    }
}
