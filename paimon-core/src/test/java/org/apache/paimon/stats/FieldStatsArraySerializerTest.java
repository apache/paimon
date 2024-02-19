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

package org.apache.paimon.stats;

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FieldStatsArraySerializer}. */
public class FieldStatsArraySerializerTest {
    @Test
    public void testFromBinary() {
        TableSchema dataSchema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new IntType()),
                                new DataField(2, "c", new IntType()),
                                new DataField(3, "d", new IntType())),
                        3,
                        Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST,
                        Collections.EMPTY_MAP,
                        "");
        TableSchema tableSchema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(1, "c", new IntType()),
                                new DataField(3, "a", new IntType()),
                                new DataField(5, "d", new IntType()),
                                new DataField(6, "e", new IntType()),
                                new DataField(7, "b", new IntType())),
                        7,
                        Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST,
                        Collections.EMPTY_MAP,
                        "");

        IndexCastMapping indexCastMapping =
                SchemaEvolutionUtil.createIndexCastMapping(
                        tableSchema.fields(), dataSchema.fields());
        int[] indexMapping = indexCastMapping.getIndexMapping();
        CastExecutor<Object, Object>[] converterMapping =
                (CastExecutor<Object, Object>[])
                        SchemaEvolutionUtil.createConvertMapping(
                                tableSchema.fields(), dataSchema.fields(), indexMapping);
        FieldStatsArraySerializer fieldStatsArraySerializer =
                new FieldStatsArraySerializer(
                        tableSchema.logicalRowType(),
                        indexMapping,
                        converterMapping,
                        indexCastMapping.getCastMapping());
        BinaryRow minRowData = row(1, 2, 3, 4);
        BinaryRow maxRowData = row(100, 99, 98, 97);
        Long[] nullCounts = new Long[] {1L, 0L, 10L, 100L};
        BinaryTableStats dataTableStats =
                new BinaryTableStats(minRowData, maxRowData, BinaryArray.fromLongArray(nullCounts));

        FieldStats[] fieldStatsArray = fieldStatsArraySerializer.fromBinary(dataTableStats, 1000L);
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
