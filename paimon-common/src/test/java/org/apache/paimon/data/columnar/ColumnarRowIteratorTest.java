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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.LongIterator;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Test for {@link ColumnarRowIterator}. */
public class ColumnarRowIteratorTest {

    @Test
    void testRowIterator() {
        Random random = new Random();
        HeapIntVector heapIntVector = new HeapIntVector(100);
        for (int i = 0; i < 100; i++) {
            heapIntVector.setInt(i, random.nextInt());
        }
        long[] positions = new long[100];
        positions[0] = random.nextInt(10);
        for (int i = 1; i < 100; i++) {
            positions[i] = positions[i - 1] + random.nextInt(100);
        }

        VectorizedColumnBatch vectorizedColumnBatch =
                new VectorizedColumnBatch(new ColumnVector[] {heapIntVector});
        vectorizedColumnBatch.setNumRows(100);
        ColumnarRowIterator rowIterator =
                new ColumnarRowIterator(
                        new Path("test"), new ColumnarRow(vectorizedColumnBatch), null);
        rowIterator.reset(LongIterator.fromArray(positions));
        assertThatCode(rowIterator::returnedPosition)
                .hasMessage("returnedPosition() is called before next()");
        rowIterator.next();
        for (int i = 0; i < random.nextInt(10); i++) {
            for (int j = 0; j < random.nextInt(9); j++) {
                rowIterator.next();
            }
            assertThat(rowIterator.returnedPosition()).isEqualTo(positions[rowIterator.index - 1]);
        }
    }
}
