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

package org.apache.paimon.format.parquet;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.EdgeAlgorithm;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.convertToPaimonRowType;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.convertToParquetMessageType;
import static org.apache.paimon.types.DataTypesTest.assertThat;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/** Test for {@link ParquetSchemaConverter}. */
public class ParquetSchemaConverterTest {

    public static final RowType ALL_TYPES =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "string", DataTypes.STRING()),
                            new DataField(1, "stringNotNull", DataTypes.STRING().notNull()),
                            new DataField(2, "boolean", DataTypes.BOOLEAN()),
                            new DataField(3, "bytes", DataTypes.BYTES()),
                            new DataField(4, "decimal(9,2)", DataTypes.DECIMAL(9, 2)),
                            new DataField(5, "decimal(18,2)", DataTypes.DECIMAL(18, 2)),
                            new DataField(6, "decimal(27,2)", DataTypes.DECIMAL(27, 2)),
                            new DataField(7, "tinyint", DataTypes.TINYINT()),
                            new DataField(8, "smallint", DataTypes.SMALLINT()),
                            new DataField(9, "int", DataTypes.INT()),
                            new DataField(10, "bigint", DataTypes.BIGINT()),
                            new DataField(11, "float", DataTypes.FLOAT()),
                            new DataField(12, "double", DataTypes.DOUBLE()),
                            new DataField(13, "date", DataTypes.DATE()),
                            new DataField(14, "time", DataTypes.TIME()),
                            new DataField(15, "timestamp(3)", DataTypes.TIMESTAMP_MILLIS()),
                            new DataField(16, "timestamp", DataTypes.TIMESTAMP()),
                            new DataField(17, "timestampLtz(3)", DataTypes.TIMESTAMP_LTZ_MILLIS()),
                            new DataField(
                                    18, "timestampLtz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()),
                            new DataField(19, "array", new ArrayType(DataTypes.STRING())),
                            new DataField(
                                    20, "map", new MapType(DataTypes.STRING(), DataTypes.STRING())),
                            new DataField(
                                    21,
                                    "row",
                                    new RowType(
                                            Arrays.asList(
                                                    new DataField(
                                                            22, "f1", DataTypes.INT().notNull()),
                                                    new DataField(23, "f2", DataTypes.STRING())))),
                            new DataField(
                                    24,
                                    "nested",
                                    new RowType(
                                            Arrays.asList(
                                                    new DataField(
                                                            25,
                                                            "f1",
                                                            new MapType(
                                                                    DataTypes.STRING(),
                                                                    new ArrayType(
                                                                            DataTypes.STRING()))),
                                                    new DataField(
                                                            26,
                                                            "f2",
                                                            new RowType(
                                                                            Arrays.asList(
                                                                                    new DataField(
                                                                                            27,
                                                                                            "f1",
                                                                                            DataTypes
                                                                                                    .INT()
                                                                                                    .notNull()),
                                                                                    new DataField(
                                                                                            28,
                                                                                            "f2",
                                                                                            DataTypes
                                                                                                    .STRING())))
                                                                    .notNull()))))));

    @Test
    public void testParquetTimestampNanosSchemaConvert() {
        MessageType messageType =
                new MessageType(
                        "origin-parquet",
                        Types.primitive(INT64, Type.Repetition.OPTIONAL)
                                .as(
                                        LogicalTypeAnnotation.timestampType(
                                                false, LogicalTypeAnnotation.TimeUnit.NANOS))
                                .named("timestamp_nanos")
                                .withId(0),
                        Types.primitive(INT64, Type.Repetition.OPTIONAL)
                                .as(
                                        LogicalTypeAnnotation.timestampType(
                                                true, LogicalTypeAnnotation.TimeUnit.NANOS))
                                .named("timestamp_ltz_nanos")
                                .withId(1));

        RowType rowType = convertToPaimonRowType(messageType);

        assertThat(
                        new RowType(
                                Arrays.asList(
                                        new DataField(0, "timestamp_nanos", DataTypes.TIMESTAMP(9)),
                                        new DataField(
                                                1,
                                                "timestamp_ltz_nanos",
                                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)))))
                .isEqualTo(rowType);
    }

    @Test
    public void testPaimonParquetSchemaConvert() {
        MessageType messageType = convertToParquetMessageType(ALL_TYPES);
        RowType rowType = convertToPaimonRowType(messageType);
        assertThat(ALL_TYPES).isEqualTo(rowType);
    }

    @Test
    public void testGeospatialLogicalTypesRoundTrip() {
        RowType expected =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "geom", DataTypes.GEOMETRY("EPSG:3857")),
                                new DataField(
                                        1,
                                        "geog",
                                        DataTypes.GEOGRAPHY("OGC:CRS84", EdgeAlgorithm.KARNEY)
                                                .notNull())));

        MessageType messageType = convertToParquetMessageType(expected);
        Type geometry = messageType.getType("geom");
        Type geography = messageType.getType("geog");

        Assertions.assertThat(geometry.asPrimitiveType().getPrimitiveTypeName()).isEqualTo(BINARY);
        Assertions.assertThat(geometry.asPrimitiveType().columnOrder().getColumnOrderName())
                .isEqualTo(ColumnOrder.ColumnOrderName.UNDEFINED);
        Assertions.assertThat(geography.asPrimitiveType().columnOrder().getColumnOrderName())
                .isEqualTo(ColumnOrder.ColumnOrderName.UNDEFINED);
        Assertions.assertThat(geometry.getLogicalTypeAnnotation())
                .isInstanceOf(LogicalTypeAnnotation.GeometryLogicalTypeAnnotation.class);
        Assertions.assertThat(
                        ((LogicalTypeAnnotation.GeometryLogicalTypeAnnotation)
                                        geometry.getLogicalTypeAnnotation())
                                .getCrs())
                .isEqualTo("EPSG:3857");
        Assertions.assertThat(
                        ((LogicalTypeAnnotation.GeographyLogicalTypeAnnotation)
                                        geography.getLogicalTypeAnnotation())
                                .getAlgorithm()
                                .name())
                .isEqualTo("KARNEY");
        assertThat(expected).isEqualTo(convertToPaimonRowType(messageType));
    }

    @Test
    public void testGeographyLogicalTypeDefaults() {
        MessageType messageType =
                new MessageType(
                        "geography-defaults",
                        Types.primitive(BINARY, Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.geographyType())
                                .named("default_geography")
                                .withId(0),
                        Types.primitive(BINARY, Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.geographyType("EPSG:4326", null))
                                .named("default_algorithm")
                                .withId(1));

        RowType expected =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "default_geography", DataTypes.GEOGRAPHY()),
                                new DataField(
                                        1, "default_algorithm", DataTypes.GEOGRAPHY("EPSG:4326"))));

        assertThat(expected).isEqualTo(convertToPaimonRowType(messageType));
    }

    // Rule 5: canonical three-level list (list -> element) with a primitive element.
    @Test
    public void testParquetListElementTypeThreeLevelPrimitive() {
        GroupType list =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(
                                Types.buildGroup(Type.Repetition.REPEATED)
                                        .optional(INT32)
                                        .named("element")
                                        .named("list"))
                        .named("arr");

        Type element = ParquetSchemaConverter.parquetListElementType(list);
        assertThat(element.isPrimitive()).isEqualTo(true);
        assertThat(element.getName()).isEqualTo("element");
    }

    // Rule 5: canonical three-level list (list -> element) with a group element.
    @Test
    public void testParquetListElementTypeThreeLevelGroupElement() {
        GroupType elementStruct =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .optional(INT32)
                        .named("x")
                        .optional(INT32)
                        .named("y")
                        .named("element");
        GroupType list =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(
                                Types.buildGroup(Type.Repetition.REPEATED)
                                        .addField(elementStruct)
                                        .named("list"))
                        .named("arr");

        Type element = ParquetSchemaConverter.parquetListElementType(list);
        assertThat(element.isPrimitive()).isEqualTo(false);
        assertThat(element.getName()).isEqualTo("element");
        assertThat(element.asGroupType().getFieldCount()).isEqualTo(2);
    }

    // Rule 1: a repeated primitive field is itself the element type.
    @Test
    public void testParquetListElementTypeTwoLevelPrimitive() {
        GroupType list =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .repeated(INT32)
                        .named("element")
                        .named("arr");

        Type element = ParquetSchemaConverter.parquetListElementType(list);
        assertThat(element.isPrimitive()).isEqualTo(true);
        assertThat(element.getName()).isEqualTo("element");
    }

    // Rule 2: a repeated group with multiple fields is itself the element type.
    @Test
    public void testParquetListElementTypeTwoLevelGroupElement() {
        GroupType elementStruct =
                Types.buildGroup(Type.Repetition.REPEATED)
                        .optional(INT32)
                        .named("x")
                        .optional(INT32)
                        .named("y")
                        .named("element");
        GroupType list =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(elementStruct)
                        .named("arr");

        Type element = ParquetSchemaConverter.parquetListElementType(list);
        assertThat(element.isPrimitive()).isEqualTo(false);
        assertThat(element.getName()).isEqualTo("element");
        assertThat(element.asGroupType().getFieldCount()).isEqualTo(2);
    }

    // Rule 3: a repeated group with a single repeated field is the element type.
    @Test
    public void testParquetListElementTypeLegacyNestedRepeatedWrapper() {
        GroupType wrapper =
                Types.buildGroup(Type.Repetition.REPEATED)
                        .repeated(INT32)
                        .named("array")
                        .named("array");
        GroupType list =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(wrapper)
                        .named("arr");

        Type element = ParquetSchemaConverter.parquetListElementType(list);
        assertThat(element.isPrimitive()).isEqualTo(false);
        assertThat(element.getName()).isEqualTo("array");
        assertThat(element.asGroupType().getFieldCount()).isEqualTo(1);
        assertThat(element.asGroupType().getType(0).getName()).isEqualTo("array");
    }

    // Rule 4: a repeated group named "array" with one field is the element type.
    @Test
    public void testParquetListElementTypeLegacyArrayWrapper() {
        GroupType arrayGroup =
                Types.buildGroup(Type.Repetition.REPEATED)
                        .optional(INT32)
                        .named("foo")
                        .named("array");
        GroupType list =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(arrayGroup)
                        .named("arr");

        Type element = ParquetSchemaConverter.parquetListElementType(list);
        assertThat(element.isPrimitive()).isEqualTo(false);
        assertThat(element.getName()).isEqualTo("array");
    }

    // Rule 4: a repeated group named "<list>_tuple" with one field is the element type.
    @Test
    public void testParquetListElementTypeLegacyListTupleWrapper() {
        GroupType tupleGroup =
                Types.buildGroup(Type.Repetition.REPEATED)
                        .required(BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("str")
                        .named("my_list_tuple");
        GroupType list =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(tupleGroup)
                        .named("my_list");

        Type element = ParquetSchemaConverter.parquetListElementType(list);
        assertThat(element.isPrimitive()).isEqualTo(false);
        assertThat(element.getName()).isEqualTo("my_list_tuple");
        assertThat(element.asGroupType().getFieldCount()).isEqualTo(1);
        assertThat(element.asGroupType().getType(0).getName()).isEqualTo("str");
    }

    // Rule 5: a repeated group with a single non-repeated field that is neither "array" nor
    // "<list>_tuple" unwraps to the single child (e.g. Hive's bag/array_element encoding).
    @Test
    public void testParquetListElementTypeLegacyBagWrapper() {
        GroupType bagGroup =
                Types.buildGroup(Type.Repetition.REPEATED)
                        .optional(INT32)
                        .named("array_element")
                        .named("bag");
        GroupType list =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(bagGroup)
                        .named("arr");

        Assertions.assertThat(ParquetSchemaConverter.isThreeLevelList(list)).isTrue();
        Assertions.assertThat(ParquetSchemaConverter.isCanonicalList(list)).isFalse();

        Type element = ParquetSchemaConverter.parquetListElementType(list);
        assertThat(element.isPrimitive()).isEqualTo(true);
        assertThat(element.getName()).isEqualTo("array_element");
    }
}
