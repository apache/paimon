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

package org.apache.paimon.mergetree;

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SortBufferWriteBuffer} validation. */
public class SortBufferWriteBufferValidationTest {

    @Test
    public void testRejectWideRowsWhenPageSizeCannotHoldFixedPart() {
        RowType.Builder valueType = RowType.builder();
        for (int i = 0; i < 9000; i++) {
            valueType.field("f" + i, DataTypes.STRING());
        }

        assertThatThrownBy(
                        () ->
                                new SortBufferWriteBuffer(
                                        RowType.builder().field("key", DataTypes.INT()).build(),
                                        valueType.build(),
                                        null,
                                        new HeapMemorySegmentPool(64 * 1024 * 3L, 64 * 1024),
                                        false,
                                        MemorySize.MAX_VALUE,
                                        128,
                                        CompressOptions.defaultOptions(),
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BinaryRow fixed-length part")
                .hasMessageContaining("page-size")
                .hasMessageContaining("at least")
                .hasMessageContaining("73152 bytes");
    }
}
