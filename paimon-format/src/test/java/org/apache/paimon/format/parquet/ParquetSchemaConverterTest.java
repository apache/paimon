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
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.convertToPaimonRowType;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.convertToParquetMessageType;
import static org.apache.paimon.types.DataTypesTest.assertThat;

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
    public void testPaimonParquetSchemaConvert() {
        MessageType messageType = convertToParquetMessageType(ALL_TYPES);
        RowType rowType = convertToPaimonRowType(messageType);
        assertThat(ALL_TYPES).isEqualTo(rowType);
    }
}
