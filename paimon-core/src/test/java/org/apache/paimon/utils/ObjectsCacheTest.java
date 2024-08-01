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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ObjectsCache}. */
public class ObjectsCacheTest {

    @Test
    public void test() throws IOException {
        Map<String, List<String>> map = new HashMap<>();
        ObjectsCache<String, String> cache =
                new ObjectsCache<>(
                        new SegmentsCache<>(1024, MemorySize.ofKibiBytes(5), Long.MAX_VALUE),
                        new StringSerializer(),
                        RowType.of(DataTypes.STRING()),
                        k -> 1L,
                        (k, size) ->
                                CloseableIterator.adapterForIterator(
                                        map.get(k).stream()
                                                .map(BinaryString::fromString)
                                                .map(GenericRow::of)
                                                .map(r -> (InternalRow) r)
                                                .iterator()));

        // test empty
        map.put("k1", Collections.emptyList());
        List<String> values = cache.read("k1", null, Filter.alwaysTrue(), Filter.alwaysTrue());
        assertThat(values).isEmpty();

        // test values
        List<String> expect = Arrays.asList("v1", "v2", "v3");
        map.put("k2", expect);
        values = cache.read("k2", null, Filter.alwaysTrue(), Filter.alwaysTrue());
        assertThat(values).containsExactlyElementsOf(expect);

        // test cache
        values = cache.read("k2", null, Filter.alwaysTrue(), Filter.alwaysTrue());
        assertThat(values).containsExactlyElementsOf(expect);

        // test filter
        values =
                cache.read(
                        "k2",
                        null,
                        Filter.alwaysTrue(),
                        r -> r.getString(0).toString().endsWith("2"));
        assertThat(values).containsExactly("v2");

        // test load filter
        expect = Arrays.asList("v1", "v2", "v3");
        map.put("k3", expect);
        values =
                cache.read(
                        "k3",
                        null,
                        r -> r.getString(0).toString().endsWith("2"),
                        Filter.alwaysTrue());
        assertThat(values).containsExactly("v2");

        // test load filter empty
        expect = Arrays.asList("v1", "v2", "v3");
        map.put("k4", expect);
        values =
                cache.read(
                        "k4",
                        null,
                        r -> r.getString(0).toString().endsWith("5"),
                        Filter.alwaysTrue());
        assertThat(values).isEmpty();

        // test read concurrently
        map.clear();
        for (int i = 0; i < 10; i++) {
            map.put(String.valueOf(i), Collections.singletonList(String.valueOf(i)));
        }
        map.keySet().stream()
                .parallel()
                .forEach(
                        k -> {
                            try {
                                assertThat(
                                                cache.read(
                                                        k,
                                                        null,
                                                        Filter.alwaysTrue(),
                                                        Filter.alwaysTrue()))
                                        .containsExactly(k);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    private static class StringSerializer extends ObjectSerializer<String> {

        public StringSerializer() {
            super(RowType.of(DataTypes.STRING()));
        }

        @Override
        public InternalRow toRow(String record) {
            return GenericRow.of(BinaryString.fromString(record));
        }

        @Override
        public String fromRow(InternalRow rowData) {
            return rowData.getString(0).toString();
        }
    }
}
