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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.VectorGlobalIndexer.TrainingBatch;
import org.apache.paimon.globalindex.VectorIndexUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import scala.Tuple2;

/** Look up retained source rows during a sequential split scan and shuffle by reservoir slot. */
final class CentroidTrainingSample implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long[] positions;
    private final int[] slots;

    CentroidTrainingSample(long[] positionsBySlot) {
        positions = positionsBySlot.clone();
        Arrays.sort(positions);
        slots = new int[positions.length];
        for (int slot = 0; slot < slots.length; slot++) {
            slots[Arrays.binarySearch(positions, positionsBySlot[slot])] = slot;
        }
    }

    private int lowerBound(long position) {
        int i = Arrays.binarySearch(positions, position);
        return i < 0 ? -i - 1 : i;
    }

    Iterator<Tuple2<Integer, float[]>> select(
            Iterator<InternalRow> input, InternalRow.FieldGetter getter, long start, long end) {
        return new Iterator<Tuple2<Integer, float[]>>() {
            private long row = start;
            private int sample = lowerBound(start);
            private final int limit = lowerBound(end);

            @Override
            public boolean hasNext() {
                return sample < limit;
            }

            @Override
            public Tuple2<Integer, float[]> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                while (row < positions[sample]) {
                    input.next();
                    row++;
                }
                row++;
                return new Tuple2<>(
                        slots[sample++],
                        VectorIndexUtils.toFloatArray(getter.getFieldOrNull(input.next())));
            }
        };
    }

    static Iterator<TrainingBatch> batches(Iterator<float[]> sorted, int sampleSize) {
        return new Iterator<TrainingBatch>() {
            private int remaining = sampleSize;

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public TrainingBatch next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                float[] first = sorted.next();
                int dimension = first.length;
                if (dimension == 0) {
                    throw new IllegalArgumentException("Empty training vector");
                }
                int count = Math.min(remaining, Math.max(1, (256 * 1024) / dimension));
                float[] batch = new float[count * dimension];
                System.arraycopy(first, 0, batch, 0, dimension);
                for (int i = 1; i < count; i++) {
                    float[] vector = sorted.next();
                    if (vector.length != dimension) {
                        throw new IllegalArgumentException(
                                "Inconsistent training vector dimensions.");
                    }
                    System.arraycopy(vector, 0, batch, i * dimension, dimension);
                }
                remaining -= count;
                return new TrainingBatch(batch, count);
            }
        };
    }
}
