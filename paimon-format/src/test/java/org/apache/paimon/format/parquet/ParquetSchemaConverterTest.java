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

    /**
     * Backward-compatibility Rules 1, 2 and 4: two-level lists (a repeated primitive, a repeated
     * struct, and a legacy {@code array} wrapper) infer a non-nullable element, because a {@code
     * REPEATED} node is never null. This matches parquet-cpp's {@code SchemaManifest} contract and
     * keeps the inferred schema symmetric with what Paimon's own writer produces.
     *
     * <p>Rule 5 (three-level wrapper) is the contrast case: the wrapper child's own nullability is
     * preserved, so an {@code OPTIONAL} element stays nullable.
     */
    @Test
    public void testInferTwoLevelListElementNotNull() {
        // Rule 1: optional group my_list (LIST) { repeated int32 element; }
        MessageType rule1 =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .addField(
                                        Types.primitive(INT32, Type.Repetition.REPEATED)
                                                .named("element")
                                                .withId(1))
                                .named("my_list")
                                .withId(0));
        assertThat(
                        new RowType(
                                Arrays.asList(
                                        new DataField(
                                                0,
                                                "my_list",
                                                new ArrayType(DataTypes.INT().notNull())))))
                .isEqualTo(convertToPaimonRowType(rule1));

        // Rule 2: optional group my_list (LIST) { repeated group element { optional int32 x;
        // optional int32 y; } }
        MessageType rule2 =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .addField(
                                        Types.buildGroup(Type.Repetition.REPEATED)
                                                .addField(
                                                        Types.primitive(
                                                                        INT32,
                                                                        Type.Repetition.OPTIONAL)
                                                                .named("x")
                                                                .withId(2))
                                                .addField(
                                                        Types.primitive(
                                                                        INT32,
                                                                        Type.Repetition.OPTIONAL)
                                                                .named("y")
                                                                .withId(3))
                                                .named("element")
                                                .withId(1))
                                .named("my_list")
                                .withId(0));
        RowType rule2Element =
                new RowType(
                        Arrays.asList(
                                new DataField(2, "x", DataTypes.INT()),
                                new DataField(3, "y", DataTypes.INT())));
        assertThat(
                        new RowType(
                                Arrays.asList(
                                        new DataField(
                                                0,
                                                "my_list",
                                                new ArrayType(rule2Element.notNull())))))
                .isEqualTo(convertToPaimonRowType(rule2));

        // Rule 4: optional group my_list (LIST) { repeated group array { optional int32 foo; } }
        MessageType rule4 =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .addField(
                                        Types.buildGroup(Type.Repetition.REPEATED)
                                                .addField(
                                                        Types.primitive(
                                                                        INT32,
                                                                        Type.Repetition.OPTIONAL)
                                                                .named("foo")
                                                                .withId(2))
                                                .named("array")
                                                .withId(1))
                                .named("my_list")
                                .withId(0));
        RowType rule4Element = new RowType(Arrays.asList(new DataField(2, "foo", DataTypes.INT())));
        assertThat(
                        new RowType(
                                Arrays.asList(
                                        new DataField(
                                                0,
                                                "my_list",
                                                new ArrayType(rule4Element.notNull())))))
                .isEqualTo(convertToPaimonRowType(rule4));

        // Rule 5 (contrast): optional group my_list (LIST) { repeated group bag { optional int32
        // array_element; } } keeps the OPTIONAL element nullable.
        MessageType rule5 =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .addField(
                                        Types.buildGroup(Type.Repetition.REPEATED)
                                                .addField(
                                                        Types.primitive(
                                                                        INT32,
                                                                        Type.Repetition.OPTIONAL)
                                                                .named("array_element")
                                                                .withId(2))
                                                .named("bag")
                                                .withId(1))
                                .named("my_list")
                                .withId(0));
        assertThat(
                        new RowType(
                                Arrays.asList(
                                        new DataField(
                                                0, "my_list", new ArrayType(DataTypes.INT())))))
                .isEqualTo(convertToPaimonRowType(rule5));
    }

    /**
     * Backward-compatibility Rule 3: an annotated list whose element is a nested legacy list infers
     * {@code ARRAY<ARRAY<INT NOT NULL> NOT NULL>}.
     */
    @Test
    public void testInferNestedLegacyList() {
        // Rule 3: optional group my_list (LIST) { repeated group element { repeated int32 array; }
        // }
        MessageType annotated =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .addField(
                                        Types.buildGroup(Type.Repetition.REPEATED)
                                                .addField(
                                                        Types.primitive(
                                                                        INT32,
                                                                        Type.Repetition.REPEATED)
                                                                .named("array")
                                                                .withId(2))
                                                .named("element")
                                                .withId(1))
                                .named("my_list")
                                .withId(0));
        assertThat(
                        new RowType(
                                Arrays.asList(
                                        new DataField(
                                                0,
                                                "my_list",
                                                new ArrayType(
                                                        new ArrayType(DataTypes.INT().notNull())
                                                                .notNull())))))
                .isEqualTo(convertToPaimonRowType(annotated));

        // Without the annotation: repeated group my_list { repeated group array { optional int32
        // x; } } infers ARRAY<ROW<x INT> NOT NULL>.
        MessageType unannotated =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.REPEATED)
                                .addField(
                                        Types.buildGroup(Type.Repetition.REPEATED)
                                                .addField(
                                                        Types.primitive(
                                                                        INT32,
                                                                        Type.Repetition.OPTIONAL)
                                                                .named("x")
                                                                .withId(2))
                                                .named("array")
                                                .withId(1))
                                .named("my_list")
                                .withId(0));
        RowType unannotatedElement =
                new RowType(Arrays.asList(new DataField(2, "x", DataTypes.INT())));
        assertThat(
                        new RowType(
                                Arrays.asList(
                                        new DataField(
                                                0,
                                                "my_list",
                                                new ArrayType(unannotatedElement.notNull())))))
                .isEqualTo(convertToPaimonRowType(unannotated));
    }
}
