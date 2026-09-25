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

package org.apache.paimon.io;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DataOutputSerializer}. */
public class DataOutputSerializerTest {

    @Test
    public void testWriteBytesAdvancesPositionOnce() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeBytes("abc");

        assertThat(out.length()).isEqualTo(3);
        assertThat(out.getCopyOfBuffer()).containsExactly('a', 'b', 'c');
    }

    @Test
    public void testWriteBytesLeavesTheNextWriteInPlace() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeBytes("abc");
        out.writeInt(42);

        // "abc" and then 42 as a big-endian int, with no gap of untouched bytes in between.
        assertThat(out.getCopyOfBuffer()).containsExactly('a', 'b', 'c', 0, 0, 0, 42);
    }

    @Test
    public void testWriteBytesAcrossAResize() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(2);
        out.writeBytes("abcdef");

        assertThat(out.getCopyOfBuffer()).containsExactly('a', 'b', 'c', 'd', 'e', 'f');
    }
}
