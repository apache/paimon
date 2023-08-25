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

package org.apache.paimon.sort.zorder;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ZOrderByteUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/** Tests for {@link ZIndexer}. */
public class ZIndexerTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testZIndexer() {
        RowType rowType = RowType.of(new IntType(), new BigIntType());

        ZIndexer zIndexer = new ZIndexer(rowType, Arrays.asList("f0", "f1"));
        zIndexer.open();

        for (int i = 0; i < 1000; i++) {
            int a = RANDOM.nextInt();
            long b = RANDOM.nextLong();

            InternalRow internalRow = GenericRow.of(a, b);

            byte[] zOrder = zIndexer.index(internalRow);

            byte[][] zCache = new byte[2][];
            ByteBuffer byteBuffer = ByteBuffer.allocate(8);
            ZOrderByteUtils.intToOrderedBytes(a, byteBuffer);
            zCache[0] = Arrays.copyOf(byteBuffer.array(), 8);

            ZOrderByteUtils.longToOrderedBytes(b, byteBuffer);
            zCache[1] = Arrays.copyOf(byteBuffer.array(), 8);

            byte[] expectedZOrder = ZOrderByteUtils.interleaveBits(zCache, 16);

            for (int j = 0; j < 16; j++) {
                Assertions.assertThat(zOrder[j]).isEqualTo(expectedZOrder[j]);
            }
        }
    }
}
