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

package org.apache.paimon.operation.commit;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.FileEntryMergeBuffer;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.ThrowingConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.manifest.FileKind.ADD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Checks equivalence of the DE base scan and its streaming producer, including conditional dedup.
 */
class StreamingDataEvolutionBaseScanTest {

    @TempDir Path temporary;

    private final Snapshot snapshot = mock(Snapshot.class);
    private final List<BinaryRow> partitions = Collections.singletonList(EMPTY_ROW);
    private final Range range = new Range(10, 19);

    @Test
    void testSupplementalBranchDeduplicatesEvenWhenItsScanIsEmpty() throws Exception {
        SimpleFileEntry first = base("base", 10L, 10L);
        Fixture fixture =
                new Fixture(Arrays.asList(first, base("base", 100L, 2L)), Collections.emptyList());
        List<ManifestEntry> delta = Collections.singletonList(delta("new", 10L));

        assertThat(fixture.streamAndMerge(delta, true)).containsExactly(first);
        assertThat(fixture.original(delta)).containsExactly(first);
        verify(fixture.scanner)
                .forEachEntryFromChangedRowRanges(
                        eq(snapshot), eq(partitions), eq(Collections.singletonList(range)), any());
        verify(fixture.scanner)
                .forEachEntryFromDataFiles(
                        eq(snapshot), eq(partitions), eq(Collections.singleton("new")), any());
    }

    @Test
    void testFirstScanSatisfyingAllReferencesKeepsDuplicateConflict() throws Exception {
        SimpleFileEntry first = base("base", 10L, 10L);
        Fixture fixture = new Fixture(Arrays.asList(first, first), Collections.emptyList());
        List<ManifestEntry> delta = Collections.singletonList(delta("base", 10L));

        assertThat(fixture.original(delta)).containsExactly(first, first);
        assertThatThrownBy(() -> fixture.streamAndMerge(delta, false))
                .isInstanceOf(IllegalStateException.class);
        verify(fixture.scanner, never())
                .forEachEntryFromDataFiles(any(), anyList(), anySet(), any());
    }

    @Test
    void testSupplementalEntriesKeepFirstPayloadAndScanOrder() throws Exception {
        SimpleFileEntry first = base("z", 10L, 10L);
        SimpleFileEntry supplemental = base("a", null, 2L);
        Fixture fixture =
                new Fixture(
                        Arrays.asList(first, base("z", 100L, 2L)),
                        Arrays.asList(supplemental, base("a", null, 3L)));
        List<ManifestEntry> delta = Collections.singletonList(delta("a", 10L));

        assertThat(fixture.streamAndMerge(delta, true)).containsExactly(first, supplemental);
        assertThat(fixture.original(delta)).containsExactly(first, supplemental);
    }

    @Test
    void testNameOnlyBranchDoesNotDeduplicate() throws Exception {
        SimpleFileEntry first = base("legacy", null, 2L);
        Fixture fixture = new Fixture(Collections.emptyList(), Arrays.asList(first, first));
        List<ManifestEntry> delta = Collections.singletonList(delta("legacy", null));

        assertThat(fixture.original(delta)).containsExactly(first, first);
        assertThatThrownBy(() -> fixture.streamAndMerge(delta, false))
                .isInstanceOf(IllegalStateException.class);
        verify(fixture.scanner, never())
                .forEachEntryFromChangedRowRanges(any(), anyList(), anyList(), any());
    }

    @Test
    void testEmptyChangesDoNotScan() throws Exception {
        Fixture fixture = new Fixture(Collections.emptyList(), Collections.emptyList());

        assertThat(fixture.streamAndMerge(Collections.emptyList(), false)).isEmpty();
        verifyNoInteractions(fixture.scanner);
    }

    @Test
    void testConsumerFailureStopsBeforeSupplementalScan() throws Exception {
        Fixture fixture =
                new Fixture(
                        Collections.singletonList(base("base", 10L, 10L)), Collections.emptyList());
        IOException failure = new IOException("base consumer failed");

        assertThatThrownBy(
                        () ->
                                fixture.detection.forEachBaseDataFile(
                                        snapshot,
                                        partitions,
                                        Collections.singletonList(delta("new", 10L)),
                                        Collections.emptyList(),
                                        entry -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);
        verify(fixture.scanner, never())
                .forEachEntryFromDataFiles(any(), anyList(), anySet(), any());
    }

    private final class Fixture {
        private final CommitScanner scanner = mock(CommitScanner.class);
        private final DataEvolutionConflictDetection detection =
                new DataEvolutionConflictDetection(
                        "test",
                        "writer",
                        RowType.of(),
                        null,
                        BucketMode.BUCKET_UNAWARE,
                        false,
                        false,
                        null,
                        null,
                        scanner);

        private Fixture(List<SimpleFileEntry> byRange, List<SimpleFileEntry> byName)
                throws Exception {
            when(scanner.readAllEntriesFromChangedRowRanges(
                            eq(snapshot), eq(partitions), anyList()))
                    .thenReturn(byRange);
            when(scanner.readAllEntriesFromDataFiles(eq(snapshot), eq(partitions), anySet()))
                    .thenReturn(byName);
            doAnswer(
                            invocation -> {
                                ThrowingConsumer<SimpleFileEntry, Exception> consumer =
                                        invocation.getArgument(3);
                                for (SimpleFileEntry entry : byRange) {
                                    consumer.accept(entry);
                                }
                                return null;
                            })
                    .when(scanner)
                    .forEachEntryFromChangedRowRanges(
                            eq(snapshot), eq(partitions), anyList(), any());
            doAnswer(
                            invocation -> {
                                ThrowingConsumer<SimpleFileEntry, Exception> consumer =
                                        invocation.getArgument(3);
                                for (SimpleFileEntry entry : byName) {
                                    consumer.accept(entry);
                                }
                                return null;
                            })
                    .when(scanner)
                    .forEachEntryFromDataFiles(eq(snapshot), eq(partitions), anySet(), any());
        }

        private List<SimpleFileEntry> original(List<ManifestEntry> delta) {
            return detection.scanBaseDataFiles(
                    snapshot,
                    partitions,
                    delta,
                    Collections.emptyList(),
                    Snapshot.CommitKind.APPEND,
                    null,
                    false);
        }

        private List<SimpleFileEntry> streamAndMerge(
                List<ManifestEntry> delta, boolean expectedDeduplication) throws Exception {
            List<SimpleFileEntry> result = new ArrayList<>();
            try (IOManager io = IOManager.create(temporary.toString());
                    FileEntryMergeBuffer buffer =
                            new FileEntryMergeBuffer(new CoreOptions(Collections.emptyMap()), io)) {
                boolean deduplicate =
                        detection.forEachBaseDataFile(
                                snapshot,
                                partitions,
                                delta,
                                Collections.emptyList(),
                                buffer::writeBase);
                assertThat(deduplicate).isEqualTo(expectedDeduplication);
                buffer.forEachMerged(deduplicate, result::add);
            }
            return result;
        }
    }

    private ManifestEntry delta(String name, Long first) {
        ManifestEntry entry = mock(ManifestEntry.class);
        DataFileMeta file = mock(DataFileMeta.class);
        when(entry.kind()).thenReturn(ADD);
        when(entry.file()).thenReturn(file);
        when(file.fileName()).thenReturn(name);
        when(file.firstRowId()).thenReturn(first);
        if (first != null) {
            when(file.nonNullRowIdRange()).thenReturn(range);
        }
        return entry;
    }

    private static SimpleFileEntry base(String name, Long first, long count) {
        return new SimpleFileEntry(
                ADD,
                EMPTY_ROW,
                0,
                1,
                0,
                name,
                Collections.emptyList(),
                null,
                EMPTY_ROW,
                EMPTY_ROW,
                null,
                count,
                first);
    }
}
