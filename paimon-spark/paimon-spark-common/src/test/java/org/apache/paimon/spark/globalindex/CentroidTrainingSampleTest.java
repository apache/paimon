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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.CentroidVectorGlobalIndexer.TrainingBatch;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.IntStream;

import scala.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

class CentroidTrainingSampleTest {
    @Test
    void restoresSlotOrderAcrossEmptyAndUnevenSplits() {
        long[] bySlot = {155, 0, 99, 98, 156, 10};
        Tuple2<long[], int[]> plan = CentroidTrainingSample.plan(bySlot);
        TreeMap<Integer, float[]> shuffled = new TreeMap<>();
        int start = 0;
        for (int end : new int[] {0, 1, 3, 98, 98, 156, 200}) {
            Iterator<InternalRow> rows =
                    IntStream.range(start, end)
                            .<InternalRow>mapToObj(i -> GenericRow.of(new float[] {i}))
                            .iterator();
            CentroidTrainingSample.select(
                            plan, rows, row -> ((GenericRow) row).getField(0), start, end)
                    .forEachRemaining(
                            pair -> {
                                assertThat(shuffled.put(pair._1(), pair._2())).isNull();
                            });
            start = end;
        }
        assertThat(shuffled.keySet()).containsExactly(0, 1, 2, 3, 4, 5);
        Iterator<TrainingBatch> batches =
                CentroidTrainingSample.batches(shuffled.values().iterator(), bySlot.length);
        assertThat(batches.next().vectors).containsExactly(155, 0, 99, 98, 156, 10);
        assertThat(batches.hasNext()).isFalse();
    }

    @Test
    void batchesCrossMapBoundariesWithoutChangingRowOrder() {
        int count = 270007;
        Iterator<float[]> rows =
                IntStream.range(0, count).mapToObj(i -> new float[] {i, -i}).iterator();
        Iterator<TrainingBatch> batches = CentroidTrainingSample.batches(rows, count);
        int ordinal = 0;
        List<Integer> sizes = new ArrayList<>();
        while (batches.hasNext()) {
            TrainingBatch batch = batches.next();
            sizes.add(batch.count);
            for (int i = 0; i < batch.count; i++, ordinal++) {
                if (batch.vectors[2 * i] != ordinal || batch.vectors[2 * i + 1] != -ordinal) {
                    throw new AssertionError("Reordered sample at " + ordinal);
                }
            }
        }
        assertThat(sizes).containsExactly(131072, 131072, count - 262144);
        assertThat(ordinal).isEqualTo(count);
    }
}
