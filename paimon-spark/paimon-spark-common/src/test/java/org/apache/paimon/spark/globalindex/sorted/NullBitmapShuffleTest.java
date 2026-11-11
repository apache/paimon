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

package org.apache.paimon.spark.globalindex.sorted;

import org.apache.paimon.globalindex.sorted.NullBitmapBuffer;
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.spark.Partitioner;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NullBitmapShuffleTest {

    @Test
    void testAdaptiveCompressionThreshold() {
        assertThat(NullBitmapShuffle.shouldCompress(0, 0)).isFalse();
        assertThat(NullBitmapShuffle.shouldCompress(10000, 0)).isFalse();
        assertThat(NullBitmapShuffle.shouldCompress(10000, 7999)).isFalse();
        assertThat(NullBitmapShuffle.shouldCompress(10000, 8000)).isTrue();
        assertThat(NullBitmapShuffle.shouldCompress(10000, 10000)).isTrue();
    }

    @Test
    void testSamplingRescanDoesNotDoubleCountNullHeavyPartition() {
        assertThat(
                        NullBitmapShuffle.shouldCompress(
                                Arrays.asList(
                                        new long[] {0, 7900, 7900},
                                        new long[] {1, 2100, 0},
                                        new long[] {0, 7900, 7900})))
                .isFalse();
    }

    @Test
    void testMixedRowsAndTaskBoundaries() {
        List<Row> input =
                Arrays.asList(
                        RowFactory.create(0L, null, 1L << 40),
                        RowFactory.create(0L, 10, (1L << 40) + 1),
                        RowFactory.create(0L, null, (1L << 40) + 2),
                        RowFactory.create(1L, null, Long.MAX_VALUE));
        List<Row> compressed = collect(NullBitmapShuffle.compressRows(input.iterator()));
        assertThat(compressed).hasSize(3);
        assertThat(collect(NullBitmapShuffle.expandRows(compressed.iterator())))
                .containsExactlyInAnyOrderElementsOf(input);
    }

    @Test
    void testTransportDoesNotSerializeSourceSchemaMetadata() throws Exception {
        StructType schema =
                new StructType()
                        .add("task_id", DataTypes.LongType, false)
                        .add(
                                "index_key",
                                DataTypes.IntegerType,
                                true,
                                new MetadataBuilder()
                                        .putString("unused", new String(new char[4096]))
                                        .build())
                        .add("row_id", DataTypes.LongType, false);
        Row input = new GenericRowWithSchema(new Object[] {0L, 10, 1L}, schema);
        Row transport =
                NullBitmapShuffle.compressRows(Collections.singletonList(input).iterator()).next();
        assertThat(transport).isEqualTo(input);
        // Only the three scalar values belong in the Java-serialized shuffle payload.
        assertThat(InstantiationUtil.serializeObject(transport).length).isLessThan(1024);
    }

    @Test
    void testAllNullBatchesAreBounded() {
        List<Row> input = new ArrayList<>();
        for (long i = 0; i < NullBitmapBuffer.MAX_ROWS * 3L + 1; i++) {
            input.add(RowFactory.create(0L, null, i));
        }
        List<Row> compressed = collect(NullBitmapShuffle.compressRows(input.iterator()));
        assertThat(compressed).hasSize(4);
        assertThat(collect(NullBitmapShuffle.expandRows(compressed.iterator())))
                .containsExactlyElementsOf(input);
    }

    @Test
    void testBatchesNeverCrossExistingPartitionBoundaries() {
        Partitioner partitioner =
                new Partitioner() {
                    @Override
                    public int numPartitions() {
                        return 4;
                    }

                    @Override
                    public int getPartition(Object key) {
                        return (int) (((Row) key).getLong(2) / 4000);
                    }
                };
        List<Row> input = new ArrayList<>();
        for (long i = 0; i < 16000; i++) {
            input.add(RowFactory.create(0L, null, i));
        }
        List<Row> compressed =
                collect(NullBitmapShuffle.compressRows(input.iterator(), partitioner));
        assertThat(compressed).hasSize(4);
        for (Row row : compressed) {
            int target = partitioner.getPartition(row);
            List<Row> expanded =
                    collect(
                            NullBitmapShuffle.expandRows(
                                    Collections.singletonList(row).iterator()));
            assertThat(expanded).hasSize(4000);
            for (Row original : expanded) {
                assertThat(partitioner.getPartition(original)).isEqualTo(target);
            }
        }
    }

    @Test
    void testEmptyAndNonNullRows() {
        assertThat(
                        NullBitmapShuffle.compressRows(Collections.<Row>emptyList().iterator())
                                .hasNext())
                .isFalse();
        List<Row> input = Arrays.asList(RowFactory.create(0L, 1, 0L), RowFactory.create(0L, 2, 1L));
        List<Row> compressed = collect(NullBitmapShuffle.compressRows(input.iterator()));
        assertThat(compressed).hasSize(2);
        assertThat(collect(NullBitmapShuffle.expandRows(compressed.iterator())))
                .containsExactlyElementsOf(input);
    }

    private static List<Row> collect(Iterator<Row> iterator) {
        List<Row> rows = new ArrayList<>();
        iterator.forEachRemaining(rows::add);
        return rows;
    }
}
