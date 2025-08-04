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

package org.apache.paimon.flink.action.cdc.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link DfsSort}. */
public class DfsSortTestTest {
    @Test
    public void testSortKeys() {
        LinkedHashMap<String, String> refs = new LinkedHashMap<>();
        refs.put("A", "B");
        refs.put("B", "O");
        refs.put("C", null);
        refs.put("D", "A");
        refs.put("E", "C");
        refs.put("F", "");

        List<String> sorted = DfsSort.sortKeys(refs);
        assertEquals(Arrays.asList("B", "C", "F", "E", "A", "D"), sorted);
    }

    @Test
    public void testCycleReference() {
        LinkedHashMap<String, String> refs = new LinkedHashMap<>();
        refs.put("A", "B");
        refs.put("B", "C");
        refs.put("C", "A");

        assertThrows(IllegalArgumentException.class, () -> DfsSort.sort(refs));
    }
}
