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

package org.apache.paimon.flink;

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;

import org.apache.flink.table.data.ArrayData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkRowData} and vector type. */
public class FlinkRowDataWithVectorTest {

    @Test
    public void testVectorAsArrayData() {
        float[] values = new float[] {1.0f, 2.0f, 3.0f};
        InternalRow row = GenericRow.of(1, BinaryVector.fromPrimitiveArray(values));

        FlinkRowData rowData = new FlinkRowData(row);
        ArrayData arrayData = rowData.getArray(1);

        assertThat(arrayData.toFloatArray()).isEqualTo(values);
    }

    @Test
    public void testNullVector() {
        InternalRow row = GenericRow.of(1, null);
        FlinkRowData rowData = new FlinkRowData(row);

        assertThat(rowData.isNullAt(1)).isTrue();
    }
}
