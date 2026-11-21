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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.manifest.SimpleFileEntryWithDV;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.manifest.FileKind.ADD;
import static org.apache.paimon.manifest.FileKind.DELETE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileEntryMergeBufferTest {

    @TempDir Path temporary;

    @Test
    void testPreservesInsertionOrderAfterIdentitySorting() throws Exception {
        List<SimpleFileEntry> base = Arrays.asList(entry("z", ADD), entry("a", ADD));
        List<SimpleFileEntry> delta = Arrays.asList(entry("a", DELETE), entry("m", ADD));

        assertThat(merge(base, delta)).containsExactly(entry("z", ADD), entry("m", ADD));

        SimpleFileEntry reinserted =
                new SimpleFileEntry(
                        ADD,
                        EMPTY_ROW,
                        0,
                        1,
                        0,
                        "f",
                        Collections.emptyList(),
                        null,
                        EMPTY_ROW,
                        EMPTY_ROW,
                        null,
                        3L,
                        100L);
        assertThat(
                        merge(
                                Collections.singletonList(entry("z", ADD)),
                                Arrays.asList(
                                        entry("f", ADD),
                                        entry("g", ADD),
                                        entry("f", DELETE),
                                        reinserted)))
                .containsExactly(entry("z", ADD), entry("g", ADD), reinserted);
    }

    @Test
    void testPreservesOrderedAddDeleteStateMachine() throws Exception {
        List<List<SimpleFileEntry>> histories =
                Arrays.asList(
                        Collections.singletonList(entry("f", ADD)),
                        Collections.singletonList(entry("f", DELETE)),
                        Arrays.asList(entry("f", ADD), entry("f", DELETE)),
                        Arrays.asList(entry("f", DELETE), entry("f", DELETE)),
                        Arrays.asList(entry("f", ADD), entry("f", DELETE), entry("f", ADD)),
                        Arrays.asList(entry("f", ADD), entry("f", ADD)),
                        Arrays.asList(entry("f", DELETE), entry("f", ADD)));
        for (boolean existing : new boolean[] {false, true}) {
            List<SimpleFileEntry> base =
                    existing ? Collections.singletonList(entry("f", ADD)) : Collections.emptyList();
            for (List<SimpleFileEntry> delta : histories) {
                List<SimpleFileEntry> expected;
                try {
                    FileEntry.mergeEntries(delta);
                    List<SimpleFileEntry> combined = new ArrayList<>(base);
                    combined.addAll(delta);
                    expected = new ArrayList<>(FileEntry.mergeEntries(combined));
                } catch (IllegalStateException invalidHistory) {
                    assertThatThrownBy(() -> merge(base, delta))
                            .isInstanceOf(IllegalStateException.class);
                    continue;
                }
                assertThat(merge(base, delta)).containsExactlyElementsOf(expected);
            }
        }
    }

    @Test
    void testPreservesCompleteIdentityAndDvSubtype() throws Exception {
        SimpleFileEntry plain = entry("same", ADD);
        List<SimpleFileEntry> distinct =
                Arrays.asList(
                        plain,
                        entry("same", ADD, EMPTY_ROW, 1, Collections.emptyList(), null, null),
                        entry(
                                "same",
                                ADD,
                                BinaryRow.singleColumn(7),
                                0,
                                Collections.emptyList(),
                                null,
                                null),
                        new SimpleFileEntry(
                                ADD,
                                EMPTY_ROW,
                                0,
                                1,
                                1,
                                "same",
                                Collections.emptyList(),
                                null,
                                EMPTY_ROW,
                                EMPTY_ROW,
                                null,
                                2L,
                                10L),
                        entry(
                                "same",
                                ADD,
                                EMPTY_ROW,
                                0,
                                Collections.singletonList("extra"),
                                null,
                                null),
                        entry(
                                "same",
                                ADD,
                                EMPTY_ROW,
                                0,
                                Arrays.asList("first", "second"),
                                null,
                                null),
                        entry(
                                "same",
                                ADD,
                                EMPTY_ROW,
                                0,
                                Arrays.asList("second", "first"),
                                null,
                                null),
                        entry(
                                "same",
                                ADD,
                                EMPTY_ROW,
                                0,
                                Collections.emptyList(),
                                new byte[0],
                                null),
                        entry(
                                "same",
                                ADD,
                                EMPTY_ROW,
                                0,
                                Collections.emptyList(),
                                new byte[] {1},
                                null),
                        entry("same", ADD, EMPTY_ROW, 0, Collections.emptyList(), null, ""),
                        entry("same", ADD, EMPTY_ROW, 0, Collections.emptyList(), null, "external"),
                        new SimpleFileEntryWithDV(plain, null),
                        new SimpleFileEntryWithDV(plain, "dv"),
                        entry("unicode-文件", ADD));
        List<SimpleFileEntry> actual = merge(distinct, Collections.emptyList());

        assertThat(actual).containsExactlyElementsOf(distinct);
        for (int i = 0; i < distinct.size(); i++) {
            assertThat(actual.get(i).identifier()).isEqualTo(distinct.get(i).identifier());
        }
        assertThat(actual.stream().map(FileEntry::identifier).distinct().count())
                .isEqualTo(distinct.size());
    }

    @Test
    void testPhysicalIdentityExcludesRowIdCountAndKeyRange() {
        SimpleFileEntry changedMetadata =
                new SimpleFileEntry(
                        ADD,
                        EMPTY_ROW,
                        0,
                        1,
                        0,
                        "same",
                        Collections.emptyList(),
                        null,
                        BinaryRow.singleColumn(99),
                        BinaryRow.singleColumn(101),
                        null,
                        3L,
                        100L);

        assertThatThrownBy(
                        () ->
                                merge(
                                        Collections.singletonList(entry("same", ADD)),
                                        Collections.singletonList(changedMetadata)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testStringIdentityIsLossless() throws Exception {
        SimpleFileEntry original =
                entry("same", ADD, EMPTY_ROW, 0, Collections.emptyList(), null, "x\uD800");
        SimpleFileEntry different =
                entry("same", DELETE, EMPTY_ROW, 0, Collections.emptyList(), null, "x?");
        List<SimpleFileEntry> expected =
                new ArrayList<>(FileEntry.mergeEntries(Arrays.asList(original, different)));

        assertThat(original.identifier()).isNotEqualTo(different.identifier());
        List<SimpleFileEntry> actual =
                merge(Collections.singletonList(original), Collections.singletonList(different));
        assertThat(actual).containsExactlyElementsOf(expected);
        assertThat(actual.get(0).identifier()).isEqualTo(original.identifier());
        assertThat(actual.get(1).identifier()).isEqualTo(different.identifier());
    }

    @Test
    void testDeduplicatedBaseKeepsFirstPayloadAndOriginalOrder() throws Exception {
        SimpleFileEntry first = entry("f", ADD);
        SimpleFileEntry later =
                new SimpleFileEntry(
                        ADD,
                        EMPTY_ROW,
                        0,
                        1,
                        0,
                        "f",
                        Collections.emptyList(),
                        null,
                        EMPTY_ROW,
                        EMPTY_ROW,
                        null,
                        3L,
                        100L);
        List<SimpleFileEntry> base = Arrays.asList(entry("z", ADD), first, entry("g", ADD), later);
        List<SimpleFileEntry> delta = Collections.singletonList(entry("h", ADD));

        assertThatThrownBy(() -> merge(base, delta)).isInstanceOf(IllegalStateException.class);
        assertThat(merge(base, delta, true))
                .containsExactly(entry("z", ADD), first, entry("g", ADD), entry("h", ADD));
    }

    @Test
    void testDeduplicatedBaseDoesNotDeduplicateDelta() {
        List<SimpleFileEntry> base = Arrays.asList(entry("f", ADD), entry("f", ADD));
        List<SimpleFileEntry> delta = Arrays.asList(entry("g", ADD), entry("g", ADD));

        assertThatThrownBy(() -> merge(base, delta, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fileName=g");
    }

    @Test
    void testObservesBaseAndLastDeleteBeforeTheyCancel() throws Exception {
        SimpleFileEntry current =
                new SimpleFileEntry(
                        ADD,
                        EMPTY_ROW,
                        0,
                        1,
                        0,
                        "f",
                        Collections.emptyList(),
                        null,
                        EMPTY_ROW,
                        EMPTY_ROW,
                        null,
                        3L,
                        100L);
        SimpleFileEntry stale = entry("f", DELETE);
        List<SimpleFileEntry> observed = new ArrayList<>();
        List<SimpleFileEntry> merged = new ArrayList<>();
        try (IOManager io = IOManager.create(temporary.toString());
                FileEntryMergeBuffer buffer = new FileEntryMergeBuffer(options(), io)) {
            buffer.writeBase(current);
            buffer.writeDelta(current.toDelete());
            buffer.writeDelta(current.toDelete());
            buffer.writeDelta(stale);
            buffer.forEachMerged(
                    false,
                    (base, ordinal, lastDelete) -> {
                        assertThat(ordinal).isZero();
                        observed.add(base);
                        observed.add(lastDelete);
                    },
                    merged::add);
        }

        assertThat(merged).isEmpty();
        assertThat(observed).containsExactly(current, stale);
    }

    @Test
    void testBaseObservationKeepsFirstPayloadOrdinalAndIndependentRecords() throws Exception {
        SimpleFileEntry first = entry("f", ADD);
        SimpleFileEntry later =
                new SimpleFileEntry(
                        ADD,
                        EMPTY_ROW,
                        0,
                        1,
                        0,
                        "f",
                        Collections.emptyList(),
                        null,
                        EMPTY_ROW,
                        EMPTY_ROW,
                        null,
                        3L,
                        100L);
        List<SimpleFileEntry> observed = new ArrayList<>();
        List<String> ordinals = new ArrayList<>();
        try (IOManager io = IOManager.create(temporary.toString());
                FileEntryMergeBuffer buffer = new FileEntryMergeBuffer(options(), io)) {
            buffer.writeBase(entry("z", ADD));
            buffer.writeBase(first);
            buffer.writeBase(later);
            buffer.writeDelta(entry("delta-only", ADD));
            buffer.forEachMerged(
                    true,
                    (base, ordinal, lastDelete) -> {
                        observed.add(base);
                        ordinals.add(base.fileName() + ":" + ordinal);
                        assertThat(lastDelete).isNull();
                    },
                    ignored -> {});
        }

        assertThat(observed).containsExactlyInAnyOrder(entry("z", ADD), first);
        assertThat(ordinals).containsExactlyInAnyOrder("z:0", "f:1");
    }

    @Test
    void testBaseObserverFailureClosesSpillFiles() throws Exception {
        IOException failure = new IOException("base observer failed");
        try (IOManager io = IOManager.create(temporary.toString())) {
            try (FileEntryMergeBuffer buffer = new FileEntryMergeBuffer(options(), io)) {
                for (int i = 0; i < 3000; i++) {
                    buffer.writeBase(entry("file-" + i, ADD));
                }
                assertThat(regularFiles()).isNotEmpty();
                assertThatThrownBy(
                                () ->
                                        buffer.forEachMerged(
                                                false,
                                                (base, ordinal, lastDelete) -> {
                                                    throw failure;
                                                },
                                                ignored -> {}))
                        .isSameAs(failure);
            }
            assertThat(regularFiles()).isEmpty();
        }
    }

    @Test
    void testSpillsAndEmitsOriginalOrder() throws Exception {
        try (IOManager io = IOManager.create(temporary.toString())) {
            try (FileEntryMergeBuffer buffer = new FileEntryMergeBuffer(options(), io)) {
                for (int i = 20000; i > 0; i--) {
                    buffer.writeBase(entry(String.format("file-%05d", i), ADD));
                }
                assertThat(regularFiles()).isNotEmpty();
                int[] expected = {20000};
                buffer.forEachMerged(
                        value -> {
                            assertThat(value.fileName())
                                    .isEqualTo(String.format("file-%05d", expected[0]--));
                            assertThat(value.kind()).isEqualTo(ADD);
                        });
                assertThat(expected[0]).isZero();
            }
            assertThat(regularFiles()).isEmpty();
        }
    }

    @Test
    void testConsumerFailureCleansOwnedFilesAndKeepsBorrowedManager() throws Exception {
        try (IOManager io = IOManager.create(temporary.toString())) {
            List<Path> ownedByIo;
            IOException failure = new IOException("range consumer failed");
            try (FileEntryMergeBuffer buffer = new FileEntryMergeBuffer(options(), io)) {
                for (int i = 0; i < 3000; i++) {
                    buffer.writeBase(entry("file-" + i, ADD));
                }
                assertThat(regularFiles()).isNotEmpty();
                try (Stream<Path> paths = Files.list(temporary)) {
                    ownedByIo = paths.filter(Files::isDirectory).collect(Collectors.toList());
                }
                assertThatThrownBy(
                                () ->
                                        buffer.forEachMerged(
                                                value -> {
                                                    throw failure;
                                                }))
                        .isSameAs(failure);
            }
            assertThat(regularFiles()).isEmpty();
            assertThat(ownedByIo).isNotEmpty().allMatch(Files::isDirectory);
        }
    }

    @Test
    void testEncodedMetadataBudgetFailureCleansFiles() throws Exception {
        Options limited = options().toConfiguration();
        limited.set(CoreOptions.DATA_EVOLUTION_METADATA_MEMORY_MAX_DISK_SIZE, new MemorySize(8));
        try (IOManager io = IOManager.create(temporary.toString())) {
            try (FileEntryMergeBuffer buffer =
                    new FileEntryMergeBuffer(new CoreOptions(limited), io)) {
                assertThatThrownBy(() -> buffer.writeBase(entry("exceeds-budget", ADD)))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("Metadata merge encoded limit");
            }
            assertThat(regularFiles()).isEmpty();
        }
    }

    @Test
    void testRejectsConsumptionAndWritesAfterFinishOrClose() throws Exception {
        try (IOManager io = IOManager.create(temporary.toString())) {
            FileEntryMergeBuffer buffer = new FileEntryMergeBuffer(options(), io);
            try {
                buffer.forEachMerged(
                        value -> {
                            throw new AssertionError("empty input");
                        });
                assertThatThrownBy(() -> buffer.writeDelta(entry("late", ADD)))
                        .isInstanceOf(IllegalStateException.class);
                assertThatThrownBy(() -> buffer.forEachMerged(value -> {}))
                        .isInstanceOf(IllegalStateException.class);
            } finally {
                buffer.close();
            }
            buffer.close();
            assertThatThrownBy(() -> buffer.writeBase(entry("closed", ADD)))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> buffer.forEachMerged(value -> {}))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    private List<SimpleFileEntry> merge(List<SimpleFileEntry> base, List<SimpleFileEntry> delta)
            throws Exception {
        return merge(base, delta, false);
    }

    private List<SimpleFileEntry> merge(
            List<SimpleFileEntry> base, List<SimpleFileEntry> delta, boolean deduplicateBase)
            throws Exception {
        List<SimpleFileEntry> result = new ArrayList<>();
        try (IOManager io = IOManager.create(temporary.toString());
                FileEntryMergeBuffer buffer = new FileEntryMergeBuffer(options(), io)) {
            for (SimpleFileEntry entry : base) {
                buffer.writeBase(entry);
            }
            for (SimpleFileEntry entry : delta) {
                buffer.writeDelta(entry);
            }
            buffer.forEachMerged(deduplicateBase, result::add);
        }
        return result;
    }

    private CoreOptions options() {
        Options options = new Options();
        options.set(CoreOptions.SORT_SPILL_BUFFER_SIZE, MemorySize.parse("256 kb"));
        options.set(CoreOptions.PAGE_SIZE, MemorySize.parse("4 kb"));
        return new CoreOptions(options);
    }

    private List<Path> regularFiles() throws IOException {
        try (Stream<Path> paths = Files.walk(temporary)) {
            return paths.filter(Files::isRegularFile).collect(Collectors.toList());
        }
    }

    private SimpleFileEntry entry(String name, FileKind kind) {
        return entry(name, kind, EMPTY_ROW, 0, Collections.emptyList(), null, null);
    }

    private SimpleFileEntry entry(
            String name,
            FileKind kind,
            BinaryRow partition,
            int bucket,
            List<String> extra,
            byte[] index,
            String externalPath) {
        return new SimpleFileEntry(
                kind,
                partition,
                bucket,
                1,
                0,
                name,
                extra,
                index,
                EMPTY_ROW,
                EMPTY_ROW,
                externalPath,
                2L,
                10L);
    }
}
