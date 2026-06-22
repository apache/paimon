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
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.convertToPaimonRowType;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.convertToParquetMessageType;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.createTimestampWithLogicalType;
import static org.apache.paimon.types.DataTypesTest.assertThat;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
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
        // TIMESTAMP(n<=3) is written with a MICROS annotation (for Iceberg v2 compatibility) and
        // therefore reads back as TIMESTAMP(6). All other types round-trip exactly.
        RowType expected =
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
                                new DataField(15, "timestamp(3)", new TimestampType(6)),
                                new DataField(16, "timestamp", DataTypes.TIMESTAMP()),
                                new DataField(
                                        17, "timestampLtz(3)", new LocalZonedTimestampType(6)),
                                new DataField(
                                        18,
                                        "timestampLtz",
                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()),
                                new DataField(19, "array", new ArrayType(DataTypes.STRING())),
                                new DataField(
                                        20,
                                        "map",
                                        new MapType(DataTypes.STRING(), DataTypes.STRING())),
                                new DataField(
                                        21,
                                        "row",
                                        new RowType(
                                                Arrays.asList(
                                                        new DataField(
                                                                22,
                                                                "f1",
                                                                DataTypes.INT().notNull()),
                                                        new DataField(
                                                                23, "f2", DataTypes.STRING())))),
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
                                                                                DataTypes
                                                                                        .STRING()))),
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
        assertThat(expected).isEqualTo(rowType);
    }

    @Test
    public void testLowPrecisionTimestampUseMicrosAnnotation() {
        // TIMESTAMP(n<=3) must emit a MICROS Parquet annotation, not MILLIS, so that Iceberg v2
        // readers (e.g. Athena, Trino) can interpret the column as "timestamp"/"timestamptz".
        // The Iceberg v2 spec only allows INT64 MICROS for those logical types; MILLIS is
        // Iceberg v3 only (https://iceberg.apache.org/spec/#parquet).
        for (int precision = 0; precision <= 3; precision++) {
            Type tsType =
                    createTimestampWithLogicalType(
                            "ts", precision, Type.Repetition.OPTIONAL, false);
            Type tsLtzType =
                    createTimestampWithLogicalType(
                            "ts_ltz", precision, Type.Repetition.OPTIONAL, true);

            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsAnnotation =
                    (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
                            tsType.getLogicalTypeAnnotation();
            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsLtzAnnotation =
                    (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
                            tsLtzType.getLogicalTypeAnnotation();

            Assertions.assertThat(tsAnnotation.getUnit())
                    .as("TIMESTAMP(%d) should use MICROS annotation", precision)
                    .isEqualTo(MICROS);
            Assertions.assertThat(tsLtzAnnotation.getUnit())
                    .as(
                            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(%d) should use MICROS annotation",
                            precision)
                    .isEqualTo(MICROS);
            Assertions.assertThat(tsLtzAnnotation.isAdjustedToUTC())
                    .as("TIMESTAMP_WITH_LOCAL_TIME_ZONE(%d) should be UTC-adjusted", precision)
                    .isTrue();
        }
    }
}
