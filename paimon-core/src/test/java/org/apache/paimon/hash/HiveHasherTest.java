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

package org.apache.paimon.hash;

import org.apache.paimon.bucket.HiveHasher;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.memory.MemorySegment;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HiveHasher}. */
class HiveHasherTest {

    @Test
    void testHashUnsafeBytes() {
        BinaryString binaryString = BinaryString.fromString("hello");
        assertThat(
                        HiveHasher.hashUnsafeBytes(
                                binaryString.getSegments(),
                                binaryString.getOffset(),
                                binaryString.getSizeInBytes()))
                .isEqualTo(HiveHasher.hashBytes(binaryString.toBytes()));

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            builder.append(UUID.randomUUID());
        }

        binaryString = fromString(builder.toString());
        assertThat(
                        HiveHasher.hashUnsafeBytes(
                                binaryString.getSegments(),
                                binaryString.getOffset(),
                                binaryString.getSizeInBytes()))
                .isEqualTo(HiveHasher.hashBytes(binaryString.toBytes()));
    }

    private BinaryString fromString(String input) {
        BinaryString binaryString = BinaryString.fromString(input);
        int numBytes = binaryString.getSizeInBytes();
        int pad = new Random().nextInt(100);
        int totalBytes = numBytes + pad;
        int segmentSize = totalBytes / 2 + 1;
        byte[] bytes1 = new byte[segmentSize];
        byte[] bytes2 = new byte[segmentSize];
        if (segmentSize - pad > 0 && numBytes >= segmentSize - pad) {
            binaryString.getSegments()[0].get(0, bytes1, pad, segmentSize - pad);
        }
        binaryString.getSegments()[0].get(
                segmentSize - pad, bytes2, 0, numBytes - segmentSize + pad);
        return BinaryString.fromAddress(
                new MemorySegment[] {MemorySegment.wrap(bytes1), MemorySegment.wrap(bytes2)},
                pad,
                numBytes);
    }
}
