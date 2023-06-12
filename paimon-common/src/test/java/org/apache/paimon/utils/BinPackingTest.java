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

package org.apache.paimon.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BinPacking}. */
public class BinPackingTest {

    @Test
    public void testPackForFixedBinNumber() {
        List<List<Integer>> pack =
                BinPacking.packForFixedBinNumber(
                        Arrays.asList(1, 5, 1, 2, 3, 6, 2), Integer::longValue, 3);
        assertThat(pack)
                .containsExactlyInAnyOrder(
                        Arrays.asList(1, 3), Arrays.asList(2, 5), Arrays.asList(1, 2, 6));
    }

    @Test
    public void testExactlyPack() {
        List<Pair<String, Long>> items =
                Arrays.asList(Pair.of("a", 1000L), Pair.of("b", 20L), Pair.of("c", 200L));
        List<List<Pair<String, Long>>> packed =
                BinPacking.packForOrdered(items, Pair<String, Long>::getRight, 800);
        assertThat(packed.size()).isEqualTo(2);
    }
}
