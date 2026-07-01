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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ReversedKeySerializer}. */
public class ReversedKeySerializerTest {

    private final KeySerializer serializer =
            new ReversedKeySerializer(
                    KeySerializer.create(new VarCharType(VarCharType.MAX_LENGTH)));

    @Test
    public void testRoundTrip() {
        for (String s : new String[] {"", "a", "hello", "ends_with_suffix"}) {
            BinaryString key = BinaryString.fromString(s);
            assertThat(serializer.deserialize(MemorySlice.wrap(serializer.serialize(key))))
                    .isEqualTo(key);
        }
    }

    @Test
    public void testComparatorOrdersBySuffix() {
        Comparator<Object> cmp = serializer.createComparator();
        List<BinaryString> input =
                Arrays.asList(
                        BinaryString.fromString("2b"),
                        BinaryString.fromString("1a"),
                        BinaryString.fromString("2a"),
                        BinaryString.fromString("1b"));
        List<String> sorted =
                input.stream().sorted(cmp).map(BinaryString::toString).collect(Collectors.toList());
        assertThat(sorted).containsExactly("1a", "2a", "1b", "2b");
    }
}
