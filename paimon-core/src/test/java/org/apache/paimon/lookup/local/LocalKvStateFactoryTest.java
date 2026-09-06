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

package org.apache.paimon.lookup.local;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.serializer.IntSerializer;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.ListBulkLoader;
import org.apache.paimon.lookup.ListState;
import org.apache.paimon.lookup.SetState;
import org.apache.paimon.lookup.ValueBulkLoader;
import org.apache.paimon.lookup.ValueState;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LocalKvStateFactory}. */
class LocalKvStateFactoryTest {

    @TempDir Path tempDir;

    @Test
    void testFactoryRejectsNonDirectoryPath() throws Exception {
        Path file = Files.write(tempDir.resolve("not-a-directory"), new byte[] {1});

        assertThatThrownBy(
                        () ->
                                new LocalKvStateFactory(
                                        file.toString(), options(), null, null, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("LocalKvStateFactory directory");
    }

    @Test
    void testValueStateAndStateIsolation() throws Exception {
        try (LocalKvStateFactory factory = createFactory()) {
            ValueState<Integer, Integer> first =
                    factory.valueState("first", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);
            ValueState<Integer, Integer> second =
                    factory.valueState(
                            "second", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);

            assertThat(first.get(1)).isNull();
            first.put(1, 10);
            assertThat(first.get(1)).isEqualTo(10);
            assertThat(second.get(1)).isNull();

            first.put(1, 11);
            assertThat(first.get(1)).isEqualTo(11);
            first.delete(1);
            assertThat(first.get(1)).isNull();

            assertThat(factory.preferBulkLoad()).isTrue();
            assertThatThrownBy(
                            () ->
                                    factory.valueState(
                                            "first",
                                            IntSerializer.INSTANCE,
                                            IntSerializer.INSTANCE,
                                            10))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("first");
        }
    }

    @Test
    void testValueAndSetStateSupportEmptySerializedValues() throws Exception {
        EmptyIntSerializer emptySerializer = new EmptyIntSerializer();
        try (LocalKvStateFactory factory = createFactory()) {
            @SuppressWarnings("unchecked")
            LocalKvValueState<Integer, Integer> valueState =
                    (LocalKvValueState<Integer, Integer>)
                            factory.valueState(
                                    "empty-value", IntSerializer.INSTANCE, emptySerializer, 10);
            valueState.put(1, 10);
            valueState.db.flush();
            valueState.cache.invalidateAll();
            assertThat(valueState.get(1)).isZero();

            ValueState<Integer, Integer> bulkValueState =
                    factory.valueState(
                            "bulk-empty-value", IntSerializer.INSTANCE, emptySerializer, 10);
            ValueBulkLoader valueLoader = bulkValueState.createBulkLoader();
            valueLoader.write(bulkValueState.serializeKey(2), bulkValueState.serializeValue(20));
            valueLoader.finish();
            assertThat(bulkValueState.get(2)).isZero();

            @SuppressWarnings("unchecked")
            LocalKvSetState<Integer, Integer> setState =
                    (LocalKvSetState<Integer, Integer>)
                            factory.setState(
                                    "empty-set", IntSerializer.INSTANCE, emptySerializer, 10);
            setState.add(1, 30);
            setState.db.flush();
            assertThat(setState.get(1)).containsExactly(0);
            setState.retract(1, 40);
            assertThat(setState.get(1)).isEmpty();
        }
    }

    @Test
    void testListState() throws Exception {
        try (LocalKvStateFactory factory = createFactory()) {
            ListState<Integer, Integer> state =
                    factory.listState("list", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);

            assertThat(state.get(1)).isEmpty();
            state.add(1, 3);
            state.add(1, 1);
            state.add(1, 3);
            assertThat(state.get(1)).containsExactly(3, 1, 3);

            ListState<Integer, Integer> bulkState =
                    factory.listState(
                            "bulk-list", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);
            ListBulkLoader loader = bulkState.createBulkLoader();
            loader.write(
                    bulkState.serializeKey(1),
                    Arrays.asList(
                            bulkState.serializeValue(4),
                            bulkState.serializeValue(2),
                            bulkState.serializeValue(4)));
            loader.write(
                    bulkState.serializeKey(2),
                    Collections.singletonList(bulkState.serializeValue(5)));
            loader.finish();

            assertThat(bulkState.get(1)).containsExactly(4, 2, 4);
            assertThat(bulkState.get(2)).containsExactly(5);
            bulkState.add(1, 6);
            assertThat(bulkState.get(1)).containsExactly(4, 2, 4, 6);

            ListState<Integer, Integer> duplicateState =
                    factory.listState(
                            "bulk-list-duplicate",
                            IntSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            10);
            ListBulkLoader duplicateLoader = duplicateState.createBulkLoader();
            duplicateLoader.write(
                    duplicateState.serializeKey(1),
                    Collections.singletonList(duplicateState.serializeValue(1)));
            assertThatThrownBy(
                            () ->
                                    duplicateLoader.write(
                                            duplicateState.serializeKey(1),
                                            Collections.singletonList(
                                                    duplicateState.serializeValue(2))))
                    .isInstanceOf(BulkLoader.WriteException.class)
                    .hasMessageContaining("strictly increasing");
        }
    }

    @Test
    void testListStateCachesDeserializedValuesAndInvalidatesOnAdd() throws Exception {
        CountingIntSerializer serializer = new CountingIntSerializer();
        try (LocalKvStateFactory factory = createFactory()) {
            ListState<Integer, Integer> state =
                    factory.listState("cached-list", IntSerializer.INSTANCE, serializer, 10);
            ListBulkLoader loader = state.createBulkLoader();
            loader.write(
                    state.serializeKey(1),
                    Arrays.asList(state.serializeValue(10), state.serializeValue(20)));
            loader.finish();

            assertThat(state.get(1)).containsExactly(10, 20);
            assertThat(serializer.deserializationCount).isEqualTo(2);
            assertThat(state.get(1)).containsExactly(10, 20);
            assertThat(serializer.deserializationCount).isEqualTo(2);

            state.add(1, 30);
            assertThat(state.get(1)).containsExactly(10, 20, 30);
            assertThat(serializer.deserializationCount).isEqualTo(5);
            assertThat(state.get(1)).containsExactly(10, 20, 30);
            assertThat(serializer.deserializationCount).isEqualTo(5);
        }
    }

    @Test
    void testListStateCollectsMemTableDeltasLazily() throws Exception {
        try (LocalKvStateFactory factory = createFactory()) {
            @SuppressWarnings("unchecked")
            LocalKvListState<Integer, Integer> state =
                    (LocalKvListState<Integer, Integer>)
                            factory.listState(
                                    "batched-list",
                                    IntSerializer.INSTANCE,
                                    IntSerializer.INSTANCE,
                                    10);
            List<Integer> firstExpected = new ArrayList<>();
            List<Integer> secondExpected = new ArrayList<>();
            for (int value = 0; value < 100; value++) {
                state.add(1, value);
                state.add(2, 1_000 + value);
                firstExpected.add(value);
                secondExpected.add(1_000 + value);
            }
            for (int value = 0; value < 32; value++) {
                state.add(3, value);
            }
            for (int value = 0; value < 33; value++) {
                state.add(4, value);
            }

            assertThat(rawEntries(state, 1)).hasSize(1);
            assertThat(rawEntries(state, 2)).hasSize(1);
            assertThat(rawEntries(state, 3)).hasSize(1);
            assertThat(rawEntries(state, 4)).hasSize(1);
            assertThat(state.get(1)).containsExactlyElementsOf(firstExpected);
            assertThat(state.get(2)).containsExactlyElementsOf(secondExpected);
            assertThat(state.get(3)).containsExactlyElementsOf(firstExpected.subList(0, 32));
            assertThat(state.get(4)).containsExactlyElementsOf(firstExpected.subList(0, 33));

            state.db.flush();
            state.cache.invalidateAll();
            assertThat(rawEntries(state, 1)).hasSize(1);
            assertThat(rawEntries(state, 2)).hasSize(1);
            assertThat(rawEntries(state, 3)).hasSize(1);
            assertThat(rawEntries(state, 4)).hasSize(1);
            assertThat(state.get(1)).containsExactlyElementsOf(firstExpected);
            assertThat(state.get(2)).containsExactlyElementsOf(secondExpected);
            assertThat(state.get(3)).containsExactlyElementsOf(firstExpected.subList(0, 32));
            assertThat(state.get(4)).containsExactlyElementsOf(firstExpected.subList(0, 33));
        }
    }

    @Test
    void testListStateMergesFragmentsDuringFlushAndCompaction() throws Exception {
        try (LocalKvStateFactory factory = createFactory()) {
            @SuppressWarnings("unchecked")
            LocalKvListState<Integer, Integer> state =
                    (LocalKvListState<Integer, Integer>)
                            factory.listState(
                                    "merged-list",
                                    IntSerializer.INSTANCE,
                                    IntSerializer.INSTANCE,
                                    10);
            ListBulkLoader loader = state.createBulkLoader();
            loader.write(
                    state.serializeKey(1),
                    Arrays.asList(state.serializeValue(10), state.serializeValue(20)));
            loader.write(
                    state.serializeKey(2), Collections.singletonList(state.serializeValue(40)));
            loader.finish();

            state.add(1, 30);
            state.add(1, 31);
            state.add(2, 41);
            state.db.flush();
            state.cache.invalidateAll();

            assertThat(state.get(1)).containsExactly(10, 20, 30, 31);
            assertThat(state.get(2)).containsExactly(40, 41);
            assertThat(rawEntries(state, 1)).hasSize(2);
            assertThat(rawEntries(state, 2)).hasSize(2);

            state.db.compact();
            state.cache.invalidateAll();
            assertThat(state.get(1)).containsExactly(10, 20, 30, 31);
            assertThat(state.get(2)).containsExactly(40, 41);
            assertThat(rawEntries(state, 1)).hasSize(1);
            assertThat(rawEntries(state, 2)).hasSize(1);
        }
    }

    @Test
    void testListStateWithTtlMergesFragmentsDuringFlush() throws Exception {
        try (LocalKvStateFactory factory =
                new LocalKvStateFactory(
                        tempDir.resolve("list-ttl-merge").toString(),
                        options(),
                        Duration.ofHours(1),
                        null,
                        false)) {
            @SuppressWarnings("unchecked")
            LocalKvListState<Integer, Integer> state =
                    (LocalKvListState<Integer, Integer>)
                            factory.listState(
                                    "list", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);

            state.add(1, 10);
            state.add(1, 20);
            state.db.flush();
            state.cache.invalidateAll();

            assertThat(rawEntries(state, 1)).hasSize(1);
            assertThat(state.get(1)).containsExactly(10, 20);
        }
    }

    @Test
    void testListStateTtlIsRefreshedAcrossRepeatedFlushes() throws Exception {
        AtomicLong clock = new AtomicLong(1_000);
        try (LocalKvStateFactory factory =
                new LocalKvStateFactory(
                        tempDir.resolve("list-ttl-repeated-flush").toString(),
                        options(),
                        Duration.ofMillis(100),
                        null,
                        false,
                        clock::get)) {
            @SuppressWarnings("unchecked")
            LocalKvListState<Integer, Integer> state =
                    (LocalKvListState<Integer, Integer>)
                            factory.listState(
                                    "list", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);

            state.add(1, 10);
            state.add(1, 20);
            state.db.flush();
            clock.addAndGet(50);
            state.add(1, 30);
            state.db.flush();
            clock.addAndGet(50);

            state.db.compact();
            state.cache.invalidateAll();
            assertThat(state.get(1)).containsExactly(10, 20, 30);
        }
    }

    @Test
    void testListMergeRefreshesTtlIncludingExpiredFragments() throws Exception {
        AtomicLong clock = new AtomicLong(1_000);
        LocalKvValueCodec valueCodec = new LocalKvValueCodec(Duration.ofMillis(100), clock::get);
        LocalKvListValueCodec listValueCodec = new LocalKvListValueCodec();
        LocalKvListMergeOperator mergeOperator = new LocalKvListMergeOperator(valueCodec);

        byte[] first = valueCodec.encode(listValueCodec.encodeSingle(new byte[] {0, 0, 0, 10}));
        clock.set(1_050);
        byte[] second = valueCodec.encode(listValueCodec.encodeSingle(new byte[] {0, 0, 0, 20}));
        clock.set(1_200);
        assertThat(valueCodec.isExpired(first)).isTrue();
        assertThat(valueCodec.isExpired(second)).isTrue();
        assertThat(valueCodec.valueOffset(first, 0, first.length)).isPositive();
        assertThat(valueCodec.valueOffset(second, 0, second.length)).isPositive();

        byte[] merged = mergeOperator.merge(Arrays.asList(first, second));
        int valueOffset = valueCodec.valueOffset(merged, 0, merged.length);
        assertThat(valueOffset).isPositive();
        assertThat(valueCodec.isExpired(merged)).isFalse();
        List<Integer> values = new ArrayList<>();
        listValueCodec.decode(
                merged, valueOffset, merged.length - valueOffset, IntSerializer.INSTANCE, values);
        assertThat(values).containsExactly(10, 20);

        clock.set(1_299);
        assertThat(valueCodec.isExpired(merged)).isFalse();
        clock.set(1_300);
        assertThat(valueCodec.isExpired(merged)).isTrue();
        assertThat(valueCodec.valueOffset(merged, 0, merged.length)).isPositive();
    }

    @Test
    void testSetState() throws Exception {
        try (LocalKvStateFactory factory = createFactory()) {
            SetState<Integer, Integer> state =
                    factory.setState("set", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);

            assertThat(state.get(1)).isEmpty();
            state.retract(1, 9);
            state.add(1, 3);
            state.add(1, 1);
            state.add(1, 3);
            state.add(1, 2);
            assertThat(state.get(1)).containsExactly(1, 2, 3);

            state.retract(1, 2);
            assertThat(state.get(1)).containsExactly(1, 3);
            for (int value = 4; value < 260; value++) {
                state.add(1, value);
            }
            assertThat(state.get(1)).hasSize(258);
            state.retract(1, 1);
            state.retract(1, 3);
            for (int value = 4; value < 260; value++) {
                state.retract(1, value);
            }
            assertThat(state.get(1)).isEmpty();
        }
    }

    @Test
    void testValueBulkLoadAndStrictOrdering() throws Exception {
        try (LocalKvStateFactory factory = createFactory()) {
            ValueState<Integer, Integer> state =
                    factory.valueState("bulk", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);
            assertThat(state.get(1)).isNull();
            ValueBulkLoader loader = state.createBulkLoader();
            loader.write(state.serializeKey(1), state.serializeValue(10));
            loader.write(state.serializeKey(2), state.serializeValue(20));
            loader.finish();

            assertThat(state.get(1)).isEqualTo(10);
            assertThat(state.get(2)).isEqualTo(20);

            ValueState<Integer, Integer> duplicateState =
                    factory.valueState(
                            "bulk-duplicate", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);
            ValueBulkLoader duplicateLoader = duplicateState.createBulkLoader();
            duplicateLoader.write(
                    duplicateState.serializeKey(1), duplicateState.serializeValue(10));
            assertThatThrownBy(
                            () ->
                                    duplicateLoader.write(
                                            duplicateState.serializeKey(1),
                                            duplicateState.serializeValue(11)))
                    .isInstanceOf(BulkLoader.WriteException.class)
                    .hasMessageContaining("strictly increasing");
        }
    }

    @Test
    void testValueStateTtlIsRemovedOnlyByCompaction() throws Exception {
        Options options = options();
        AtomicLong clock = new AtomicLong(1_000);
        try (LocalKvStateFactory factory =
                new LocalKvStateFactory(
                        tempDir.resolve("ttl").toString(),
                        options,
                        Duration.ofMillis(100),
                        null,
                        false,
                        clock::get)) {
            @SuppressWarnings("unchecked")
            LocalKvValueState<Integer, Integer> state =
                    (LocalKvValueState<Integer, Integer>)
                            factory.valueState(
                                    "ttl", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);
            state.put(1, 10);
            state.db.flush();
            assertThat(state.get(1)).isEqualTo(10);

            @SuppressWarnings("unchecked")
            LocalKvValueState<Integer, Integer> bulkState =
                    (LocalKvValueState<Integer, Integer>)
                            factory.valueState(
                                    "bulk-ttl", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);
            ValueBulkLoader loader = bulkState.createBulkLoader();
            loader.write(bulkState.serializeKey(2), bulkState.serializeValue(20));
            loader.finish();
            assertThat(bulkState.get(2)).isEqualTo(20);

            clock.addAndGet(100);
            assertThat(state.get(1)).isEqualTo(10);
            assertThat(bulkState.get(2)).isEqualTo(20);

            state.db.compact();
            bulkState.db.compact();
            assertThat(state.get(1)).isNull();
            assertThat(bulkState.get(2)).isNull();
        }
    }

    @Test
    void testUnmergedListAndSetStateTtlRemainVisibleBeforeCompaction() throws Exception {
        AtomicLong clock = new AtomicLong(1_000);
        try (LocalKvStateFactory factory =
                new LocalKvStateFactory(
                        tempDir.resolve("collection-ttl").toString(),
                        options(),
                        Duration.ofMillis(500),
                        null,
                        false,
                        clock::get)) {
            ListState<Integer, Integer> list =
                    factory.listState("list", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);
            SetState<Integer, Integer> set =
                    factory.setState("set", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);

            list.add(1, 10);
            set.add(1, 10);
            clock.addAndGet(350);
            list.add(1, 20);
            set.add(1, 20);
            clock.addAndGet(250);

            assertThat(list.get(1)).containsExactly(10, 20);
            assertThat(set.get(1)).containsExactly(10, 20);
        }
    }

    @Test
    void testSetStateTtlCompactionDropsOnlyExpiredValues() throws Exception {
        AtomicLong clock = new AtomicLong(1_000);
        try (LocalKvStateFactory factory =
                new LocalKvStateFactory(
                        tempDir.resolve("set-ttl-compaction").toString(),
                        options(),
                        Duration.ofMillis(100),
                        null,
                        false,
                        clock::get)) {
            @SuppressWarnings("unchecked")
            LocalKvSetState<Integer, Integer> state =
                    (LocalKvSetState<Integer, Integer>)
                            factory.setState(
                                    "set", IntSerializer.INSTANCE, IntSerializer.INSTANCE, 10);

            state.add(1, 10);
            state.db.flush();
            clock.addAndGet(50);
            state.add(1, 20);
            state.db.flush();
            clock.addAndGet(50);

            assertThat(state.get(1)).containsExactly(10, 20);
            state.db.compact();
            assertThat(state.get(1)).containsExactly(20);
        }
    }

    private LocalKvStateFactory createFactory() {
        return new LocalKvStateFactory(tempDir.toString(), options(), null, null, false);
    }

    private Options options() {
        Options options = new Options();
        options.set(CoreOptions.LOOKUP_CACHE_MAX_MEMORY_SIZE, MemorySize.ofMebiBytes(8));
        return options;
    }

    private static List<Map.Entry<byte[], byte[]>> rawEntries(
            LocalKvListState<Integer, Integer> state, int key) throws IOException {
        byte[] prefix = LocalKvCompositeKey.prefix(state.serializeKey(key));
        return state.db.rangeScan(prefix, LocalKvCompositeKey.upperBound(prefix));
    }

    private static class CountingIntSerializer implements Serializer<Integer> {

        private int deserializationCount;

        @Override
        public Serializer<Integer> duplicate() {
            return this;
        }

        @Override
        public Integer copy(Integer from) {
            return from;
        }

        @Override
        public void serialize(Integer record, DataOutputView target) throws IOException {
            target.writeInt(record);
        }

        @Override
        public Integer deserialize(DataInputView source) throws IOException {
            deserializationCount++;
            return source.readInt();
        }
    }

    private static class EmptyIntSerializer implements Serializer<Integer> {

        @Override
        public Serializer<Integer> duplicate() {
            return this;
        }

        @Override
        public Integer copy(Integer from) {
            return from;
        }

        @Override
        public void serialize(Integer record, DataOutputView target) {}

        @Override
        public Integer deserialize(DataInputView source) {
            return 0;
        }
    }
}
