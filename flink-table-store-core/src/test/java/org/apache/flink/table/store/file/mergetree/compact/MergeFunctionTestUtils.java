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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.utils.ReusingTestData;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test utils for {@link MergeFunction}s. */
public class MergeFunctionTestUtils {

    public static List<ReusingTestData> getExpectedForValueCount(List<ReusingTestData> input) {
        input = new ArrayList<>(input);
        Collections.sort(input);

        List<ReusingTestData> expected = new ArrayList<>();
        long c = 0;
        for (int i = 0; i < input.size(); i++) {
            ReusingTestData data = input.get(i);
            Preconditions.checkArgument(
                    data.valueKind == RowKind.INSERT,
                    "Only ADD value kind is supported for value count merge function.");
            c += data.value;
            if (i + 1 >= input.size() || data.key != input.get(i + 1).key) {
                if (c != 0) {
                    expected.add(
                            new ReusingTestData(data.key, data.sequenceNumber, RowKind.INSERT, c));
                }
                c = 0;
            }
        }
        return expected;
    }

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
