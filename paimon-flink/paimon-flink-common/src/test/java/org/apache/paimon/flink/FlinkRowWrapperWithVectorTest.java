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

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkRowWrapper} and vector type. */
public class FlinkRowWrapperWithVectorTest {

    @Test
    public void testVectorAccess() {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, 1);
        row.setField(1, new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f}));

        FlinkRowWrapper wrapper = new FlinkRowWrapper(row);
        assertThat(wrapper.getVector(1).toFloatArray()).isEqualTo(new float[] {1.0f, 2.0f, 3.0f});
    }

    @Test
    public void testNullVector() {
        GenericRowData row = new GenericRowData(1);
        row.setField(0, null);

        FlinkRowWrapper wrapper = new FlinkRowWrapper(row);
        assertThat(wrapper.isNullAt(0)).isTrue();
    }
}
