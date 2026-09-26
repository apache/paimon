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
                        null,
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
                        null,
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
                        Arrays.asList("b"),
                        null);

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
                        null,
                        null);

        assertThat(meta.lowerBounds().size()).isEqualTo(1);
        assertThat(meta.upperBounds().size()).isEqualTo(1);
        assertThat((byte[]) ((GenericMap) meta.lowerBounds()).get(1))
                .isEqualTo(new byte[] {1, 0, 0, 0});
        assertThat((byte[]) ((GenericMap) meta.upperBounds()).get(1))
                .isEqualTo(new byte[] {5, 0, 0, 0});
    }

    @Test
    @DisplayName("Test geospatial fields publish null counts but not WKB bounds")
    void testGeospatialBoundsSkipped() {
        IcebergSchema icebergSchema =
                new IcebergSchema(
                        0,
                        Arrays.asList(
                                new IcebergDataField(
                                        new DataField(1, "geom", DataTypes.GEOMETRY())),
                                new IcebergDataField(
                                        new DataField(2, "geog", DataTypes.GEOGRAPHY()))));

        byte[] wkbPoint =
                new byte[] {
                    1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xf0, 0x3f, 0, 0, 0, 0, 0, 0, 0x40
                };
        BinaryRow values = new BinaryRow(2);
        BinaryRowWriter rowWriter = new BinaryRowWriter(values);
        rowWriter.writeBinary(0, wkbPoint, 0, wkbPoint.length);
        rowWriter.writeBinary(1, wkbPoint, 0, wkbPoint.length);
        rowWriter.complete();

        BinaryArray nullCounts = new BinaryArray();
        BinaryArrayWriter arrayWriter = new BinaryArrayWriter(nullCounts, 2, 8);
        arrayWriter.writeLong(0, 1L);
        arrayWriter.writeLong(1, 2L);
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
                        null,
                        null);

        assertThat(meta.nullValueCounts().size()).isEqualTo(2);
        assertThat(((GenericMap) meta.nullValueCounts()).get(1)).isEqualTo(1L);
        assertThat(((GenericMap) meta.nullValueCounts()).get(2)).isEqualTo(2L);
        assertThat(meta.lowerBounds().size()).isZero();
        assertThat(meta.upperBounds().size()).isZero();
    }

    @Test
    @DisplayName("Null stats columns of a partial write align by the write columns")
    void testNullStatsColumnsWithWriteColsAlignByWriteSchema() {
        IcebergSchema icebergSchema =
                new IcebergSchema(
                        0,
                        Arrays.asList(
                                new IcebergDataField(1, "k", false, "int", null),
                                new IcebergDataField(2, "a", false, "int", null),
                                new IcebergDataField(3, "b", false, "int", null)));

        // partial write of (b, k) in SET-clause order: the stats row follows the
        // write-column order, so slot 0 is b and slot 1 is k, and the bounds must not
        // drift onto a
        BinaryRow values = new BinaryRow(2);
        BinaryRowWriter rowWriter = new BinaryRowWriter(values);
        rowWriter.writeInt(0, 100);
        rowWriter.writeInt(1, 1);
        rowWriter.complete();

        BinaryArray nullCounts = new BinaryArray();
        BinaryArrayWriter arrayWriter = new BinaryArrayWriter(nullCounts, 2, 8);
        arrayWriter.writeLong(0, 0L);
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
                        null,
                        Arrays.asList("b", "k"));

        assertThat(meta.lowerBounds().size()).isEqualTo(2);
        byte[] int1 = {1, 0, 0, 0};
        byte[] int100 = {100, 0, 0, 0};
        assertThat((byte[]) ((GenericMap) meta.lowerBounds()).get(3)).isEqualTo(int100);
        assertThat((byte[]) ((GenericMap) meta.lowerBounds()).get(1)).isEqualTo(int1);
        assertThat((byte[]) ((GenericMap) meta.upperBounds()).get(1)).isEqualTo(int1);
        assertThat(((GenericMap) meta.lowerBounds()).get(2)).isNull();
        assertThat(((GenericMap) meta.nullValueCounts()).get(2)).isNull();
    }

    @Test
    @DisplayName("Null stats columns of a nested partial write map leaf paths to the field")
    void testNullStatsColumnsWithNestedWriteColsMapToTopLevel() {
        IcebergSchema icebergSchema =
                new IcebergSchema(
                        0,
                        Arrays.asList(
                                new IcebergDataField(1, "k", false, "int", null),
                                // primitive here: the leaf-path mapping under test does
                                // not depend on the field's type
                                new IcebergDataField(2, "nest", false, "int", null)));

        // partial write of (nest.x, k) in SET-clause order: the write schema holds the
        // top-level nest field and k, and the stats follow that order
        BinaryRow values = new BinaryRow(2);
        BinaryRowWriter rowWriter = new BinaryRowWriter(values);
        rowWriter.setNullAt(0);
        rowWriter.writeInt(1, 7);
        rowWriter.complete();

        BinaryArray nullCounts = new BinaryArray();
        BinaryArrayWriter arrayWriter = new BinaryArrayWriter(nullCounts, 2, 8);
        arrayWriter.writeLong(0, 3L);
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
                        null,
                        Arrays.asList("nest.x", "k"));

        // the leaf path maps to the top-level field: nest's stats land on field 2, not k
        assertThat(meta.lowerBounds().size()).isEqualTo(1);
        assertThat((byte[]) ((GenericMap) meta.lowerBounds()).get(1))
                .isEqualTo(new byte[] {7, 0, 0, 0});
        assertThat(((GenericMap) meta.nullValueCounts()).get(2)).isEqualTo(3L);
        assertThat(((GenericMap) meta.nullValueCounts()).get(1)).isEqualTo(0L);
    }
}
