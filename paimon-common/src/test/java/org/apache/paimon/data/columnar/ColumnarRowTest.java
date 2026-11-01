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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link ColumnarRow}. */
public class ColumnarRowTest {

    @Test
    public void testToStringDoesNotUseHashCode() {
        HeapIntVector intVector = new HeapIntVector(1);
        intVector.setInt(0, 42);
        InternalRow row = new HeapRowVector(1, intVector).getRow(0);

        // Object#toString delegates to hashCode, which is unsupported for columnar rows
        assertThatCode(row::toString).doesNotThrowAnyException();
        assertThat(row.toString()).startsWith(ColumnarRow.class.getName());
    }
}
