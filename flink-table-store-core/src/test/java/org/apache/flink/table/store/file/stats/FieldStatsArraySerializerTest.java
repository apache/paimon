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

package org.apache.flink.table.store.file.stats;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.casting.CastExecutor;
import org.apache.flink.table.store.file.schema.AtomicDataType;
import org.apache.flink.table.store.file.schema.DataField;
import org.apache.flink.table.store.file.schema.SchemaEvolutionUtil;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.format.FieldStats;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.store.file.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FieldStatsArraySerializer}. */
public class FieldStatsArraySerializerTest {
    @Test
    public void testFromBinary() {
        TableSchema dataSchema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(
                                        0,
                                        "a",
                                        new AtomicDataType(DataTypes.INT().getLogicalType())),
                                new DataField(
                                        1,
                                        "b",
                                        new AtomicDataType(DataTypes.INT().getLogicalType())),
                                new DataField(
                                        2,
                                        "c",
                                        new AtomicDataType(DataTypes.INT().getLogicalType())),
                                new DataField(
                                        3,
                                        "d",
                                        new AtomicDataType(DataTypes.INT().getLogicalType()))),
                        3,
                        Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST,
                        Collections.EMPTY_MAP,
                        "");
        TableSchema tableSchema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(
                                        1,
                                        "c",
                                        new AtomicDataType(DataTypes.INT().getLogicalType())),
                                new DataField(
                                        3,
                                        "a",
                                        new AtomicDataType(DataTypes.INT().getLogicalType())),
                                new DataField(
                                        5,
                                        "d",
                                        new AtomicDataType(DataTypes.INT().getLogicalType())),
                                new DataField(
                                        6,
                                        "e",
                                        new AtomicDataType(DataTypes.INT().getLogicalType())),
                                new DataField(
                                        7,
                                        "b",
                                        new AtomicDataType(DataTypes.INT().getLogicalType()))),
                        7,
                        Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST,
                        Collections.EMPTY_MAP,
                        "");

        int[] indexMapping =
                SchemaEvolutionUtil.createIndexMapping(tableSchema.fields(), dataSchema.fields());
        CastExecutor<Object, Object>[] converterMapping =
                (CastExecutor<Object, Object>[])
                        SchemaEvolutionUtil.createConvertMapping(
                                tableSchema.fields(), dataSchema.fields(), indexMapping);
        FieldStatsArraySerializer fieldStatsArraySerializer =
                new FieldStatsArraySerializer(
                        tableSchema.logicalRowType(), indexMapping, converterMapping);
        BinaryRowData minRowData = row(1, 2, 3, 4);
        BinaryRowData maxRowData = row(100, 99, 98, 97);
        long[] nullCounts = new long[] {1, 0, 10, 100};
        BinaryTableStats dataTableStats = new BinaryTableStats(minRowData, maxRowData, nullCounts);

        FieldStats[] fieldStatsArray = dataTableStats.fields(fieldStatsArraySerializer, 1000L);
        assertThat(fieldStatsArray.length).isEqualTo(tableSchema.fields().size()).isEqualTo(5);
        checkFieldStats(fieldStatsArray[0], 2, 99, 0L);
        checkFieldStats(fieldStatsArray[1], 4, 97, 100L);
        checkFieldStats(fieldStatsArray[2], null, null, 1000L);
        checkFieldStats(fieldStatsArray[3], null, null, 1000L);
        checkFieldStats(fieldStatsArray[4], null, null, 1000L);
    }

    private void checkFieldStats(FieldStats fieldStats, Integer min, Integer max, Long nullCount) {
        assertThat(fieldStats.minValue()).isEqualTo(min);
        assertThat(fieldStats.maxValue()).isEqualTo(max);
        assertThat(fieldStats.nullCount()).isEqualTo(nullCount);
    }
}
