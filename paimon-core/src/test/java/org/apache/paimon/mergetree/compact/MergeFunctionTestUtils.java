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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.ReusingTestData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test utils for {@link MergeFunction}s. */
public class MergeFunctionTestUtils {

    public static List<ReusingTestData> getExpectedForDeduplicate(List<ReusingTestData> input) {
        input = new ArrayList<>(input);
        Collections.sort(input);

        List<ReusingTestData> expected = new ArrayList<>();
        for (int i = 0; i < input.size(); i++) {
            ReusingTestData data = input.get(i);
            if (i + 1 >= input.size() || data.key != input.get(i + 1).key) {
                expected.add(data);
            }
        }
        return expected;
    }

    public static List<ReusingTestData> getExpectedForPartialUpdate(List<ReusingTestData> input) {
        input = new ArrayList<>(input);
        Collections.sort(input);

        LinkedHashMap<Integer, List<ReusingTestData>> groups = new LinkedHashMap<>();
        for (ReusingTestData d : input) {
            groups.computeIfAbsent(d.key, k -> new ArrayList<>()).add(d);
        }

        List<ReusingTestData> expected = new ArrayList<>();
        for (List<ReusingTestData> group : groups.values()) {
            if (group.size() == 1) {
                // due to ReducerMergeFunctionWrapper
                expected.add(group.get(0));
            } else {
                group.stream()
                        .filter(d -> d.valueKind.isAdd())
                        .reduce((first, second) -> second)
                        .ifPresent(expected::add);
            }
        }
        return expected;
    }

    public static List<ReusingTestData> getExpectedForAggSum(List<ReusingTestData> input) {
        input = new ArrayList<>(input);
        Collections.sort(input);

        LinkedHashMap<Integer, List<ReusingTestData>> groups = new LinkedHashMap<>();
        for (ReusingTestData d : input) {
            groups.computeIfAbsent(d.key, k -> new ArrayList<>()).add(d);
        }

        List<ReusingTestData> expected = new ArrayList<>();
        for (List<ReusingTestData> group : groups.values()) {
            if (group.size() == 1) {
                // due to ReducerMergeFunctionWrapper
                expected.add(group.get(0));
            } else {
                long sum =
                        group.stream()
                                .mapToLong(d -> d.valueKind.isAdd() ? d.value : -d.value)
                                .sum();
                ReusingTestData last = group.get(group.size() - 1);
                expected.add(
                        new ReusingTestData(last.key, last.sequenceNumber, RowKind.INSERT, sum));
            }
        }
        return expected;
    }

    public static List<ReusingTestData> getExpectedForFirstRow(List<ReusingTestData> input) {
        input = new ArrayList<>(input);
        Collections.sort(input);

        List<ReusingTestData> expected = new ArrayList<>();
        for (int i = 0; i < input.size(); i++) {
            if (i == 0 || input.get(i).key != input.get(i - 1).key) {
                expected.add(input.get(i));
            }
        }

        return expected;
    }

    public static void assertKvsEquals(List<KeyValue> expected, List<KeyValue> actual) {
        assertThat(actual).hasSize(expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertKvEquals(expected.get(i), actual.get(i));
        }
    }

    public static void assertKvEquals(KeyValue expected, KeyValue actual) {
        if (expected == null) {
            assertThat(actual).isNull();
        } else {
            assertThat(actual.key()).isEqualTo(expected.key());
            assertThat(actual.sequenceNumber()).isEqualTo(expected.sequenceNumber());
            assertThat(actual.valueKind()).isEqualTo(expected.valueKind());
            assertThat(actual.value()).isEqualTo(expected.value());
        }
    }
}
