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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.manifest.FileKind.ADD;
import static org.apache.paimon.manifest.FileKind.DELETE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests bucket DV versions and final references independently of unaware file/DV checks. */
class BucketedDeletionVectorConflictTest {

    private final Snapshot snapshot = mock(Snapshot.class);

    /** Case 1: A installs the first DV on X before B publishes another DV based on its absence. */
    @ParameterizedTest
    @EnumSource(
            value = Snapshot.CommitKind.class,
            names = {"COMPACT", "OVERWRITE"})
    void testConcurrentFirstDvsForSameFile(Snapshot.CommitKind kind) {
        List<IndexManifestEntry> a = Collections.singletonList(index(ADD, dv("DA", "X")));
        List<IndexManifestEntry> b = Collections.singletonList(index(ADD, dv("DB", "X")));
        assertThat(check(detection(null), files("X"), Collections.emptyList(), a, kind)).isEmpty();
        assertThat(check(detection(null), files("X"), Collections.emptyList(), b, kind)).isEmpty();
        assertConflict(
                check(detection(dv("DA", "X")), files("X"), Collections.emptyList(), b, kind),
                "expected DV file null, current DV file DA");
    }

    /** Case 2: A and B both replace D0 for X; B must not overwrite A's committed DA. */
    @ParameterizedTest
    @EnumSource(
            value = Snapshot.CommitKind.class,
            names = {"COMPACT", "OVERWRITE"})
    void testConcurrentDvReplacementsForSameFile(Snapshot.CommitKind kind) {
        IndexFileMeta old = dv("D0", "X");
        List<IndexManifestEntry> a = Arrays.asList(index(DELETE, old), index(ADD, dv("DA", "X")));
        List<IndexManifestEntry> b = Arrays.asList(index(DELETE, old), index(ADD, dv("DB", "X")));
        assertThat(check(detection(old), files("X"), Collections.emptyList(), a, kind)).isEmpty();
        assertThat(check(detection(old), files("X"), Collections.emptyList(), b, kind)).isEmpty();
        assertConflict(
                check(detection(dv("DA", "X")), files("X"), Collections.emptyList(), b, kind),
                "expected DV file D0, current DV file DA");
    }

    @ParameterizedTest
    @EnumSource(
            value = Snapshot.CommitKind.class,
            names = {"COMPACT", "OVERWRITE"})
    void testReplacementsAcrossCommitKinds(Snapshot.CommitKind kind) {
        // Even an empty index is a real bucket version; its absence is a different state.
        IndexFileMeta old = dv("D0");
        assertThat(
                        check(
                                detection(old),
                                files("X"),
                                Collections.emptyList(),
                                Arrays.asList(index(DELETE, old), index(ADD, dv("D1", "X"))),
                                kind))
                .isEmpty();
        assertConflict(
                check(
                        detection(dv("DA", "Y")),
                        files("X", "Y"),
                        Collections.emptyList(),
                        Arrays.asList(index(DELETE, old), index(ADD, dv("DB", "X"))),
                        kind),
                "expected DV file D0, current DV file DA");
        // ADD-only legacy replacements must not silently bypass validation.
        assertConflict(
                check(
                        detection(old),
                        files("X"),
                        Collections.emptyList(),
                        Collections.singletonList(index(ADD, dv("D1", "X"))),
                        kind),
                "expected DV file null, current DV file D0");
    }

    /** Case 6, without D0: even first DVs for different files replace the same bucket index. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentFirstIndexesForDifferentFiles(boolean primaryKey) {
        assertThat(
                        check(
                                detection(null, primaryKey),
                                files("X", "Y"),
                                Collections.emptyList(),
                                Collections.singletonList(index(ADD, dv("DA", "X")))))
                .isEmpty();
        assertConflict(
                check(
                        detection(dv("DA", "X"), primaryKey),
                        files("X", "Y"),
                        Collections.emptyList(),
                        Collections.singletonList(index(ADD, dv("DB", "Y")))),
                "expected DV file null, current DV file DA");
    }

    /** Case 6, with D0: B's update to Y must not discard A's new deletion marks for X. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentDvReplacementsForDifferentFiles(boolean primaryKey) {
        IndexFileMeta old = dv("D0", "Y");
        IndexFileMeta committed = dv("DA", "X", "Y");
        List<IndexManifestEntry> a = Arrays.asList(index(DELETE, old), index(ADD, committed));
        List<IndexManifestEntry> b = Arrays.asList(index(DELETE, old), index(ADD, dv("DB", "Y")));
        assertThat(check(detection(old, primaryKey), files("X", "Y"), Collections.emptyList(), a))
                .isEmpty();
        assertThat(check(detection(old, primaryKey), files("X", "Y"), Collections.emptyList(), b))
                .isEmpty();
        assertConflict(
                check(
                        detection(committed, primaryKey),
                        files("X", "Y"),
                        Collections.emptyList(),
                        b),
                "expected DV file D0, current DV file DA");
    }

    /** Case 3, without a DV increment: B must not leave A's retained DA pointing to removed X. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRewriteWithoutIndexChanges(boolean primaryKey) {
        List<SimpleFileEntry> rewrite = Arrays.asList(file("X", DELETE), file("B", ADD));
        assertThat(
                        check(
                                detection(null, primaryKey),
                                files("X", "Y"),
                                rewrite,
                                Collections.emptyList()))
                .isEmpty();
        // A concurrent change to Y must survive an unrelated rewrite of X.
        assertThat(
                        check(
                                detection(dv("DY", "Y"), primaryKey),
                                files("X", "Y"),
                                rewrite,
                                Collections.emptyList()))
                .isEmpty();
        assertConflict(
                check(
                        detection(dv("DA", "X"), primaryKey),
                        files("X", "Y"),
                        rewrite,
                        Collections.emptyList()),
                "DV references data file X which is not present after applying changes");
    }

    @Test
    void testRetainedDvReferencesSamePhysicalFile() {
        // A metadata-only change retains the physical file addressed by the DV positions.
        assertThat(
                        check(
                                detection(dv("DA", "X")),
                                files("X"),
                                Arrays.asList(file("X", DELETE), file("X", ADD, EMPTY_ROW, 0, 1)),
                                Collections.emptyList()))
                .isEmpty();
    }

    @Test
    void testDuplicateOldDvDeletionIsRejected() {
        IndexFileMeta old = dv("D0", "X");
        IndexFileMeta next = dv("D1", "Y");
        assertConflict(
                check(
                        detection(old),
                        files("X", "Y", "Z"),
                        Collections.emptyList(),
                        Arrays.asList(index(DELETE, old), index(ADD, next), index(DELETE, old))),
                "Multiple DELETE DV entries for the same bucket");
    }

    /** Case 3, with a DV increment: B's rewrite or clear still depends on the old D0. */
    @ParameterizedTest
    @EnumSource(
            value = Snapshot.CommitKind.class,
            names = {"COMPACT", "OVERWRITE"})
    void testConcurrentDvUpdateBeforeRewriteWithIndexChanges(Snapshot.CommitKind kind) {
        IndexFileMeta old = dv("D0", "X");
        List<IndexManifestEntry> clear = Collections.singletonList(index(DELETE, old));
        List<SimpleFileEntry> rewrite = Arrays.asList(file("X", DELETE), file("Y", ADD));
        assertThat(check(detection(old), files("X"), rewrite, clear, kind)).isEmpty();
        assertConflict(
                check(detection(dv("DA", "X")), files("X"), rewrite, clear, kind),
                "expected DV file D0, current DV file DA");
        assertConflict(
                check(
                        detection(dv("DA", "X")),
                        files("X"),
                        rewrite,
                        Arrays.asList(index(DELETE, old), index(ADD, dv("DB", "Y"))),
                        kind),
                "expected DV file D0, current DV file DA");
    }

    @Test
    void testClearLegacyEmptyIndex() {
        IndexFileMeta old = dv("D0");
        List<IndexManifestEntry> clear = Collections.singletonList(index(DELETE, old));
        assertThat(check(detection(old), files("X"), Collections.emptyList(), clear)).isEmpty();
        assertConflict(
                check(detection(dv("DA", "X")), files("X"), Collections.emptyList(), clear),
                "expected DV file D0, current DV file DA");
    }

    /** Case 4: A rewrites X to Y without changing the DV version before B publishes a DV for X. */
    @ParameterizedTest
    @EnumSource(
            value = Snapshot.CommitKind.class,
            names = {"COMPACT", "OVERWRITE"})
    void testConcurrentRewriteBeforeDvUpdate(Snapshot.CommitKind kind) {
        List<IndexManifestEntry> b = Collections.singletonList(index(ADD, dv("DB", "X")));
        assertThat(check(detection(null), files("X"), Collections.emptyList(), b, kind)).isEmpty();
        assertThat(
                        check(
                                detection(null),
                                files("X"),
                                Arrays.asList(file("X", DELETE), file("Y", ADD)),
                                Collections.emptyList(),
                                kind))
                .isEmpty();
        assertConflict(
                check(detection(null), files("Y"), Collections.emptyList(), b, kind),
                "DV references data file X which is not present after applying changes");
    }

    /** Case 5: A and B rewrite X; after A commits, the ordinary file check rejects B's DELETE X. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentRewritesOfSameFile(boolean existingDv) {
        IndexFileMeta old = existingDv ? dv("D0", "X") : null;
        List<IndexManifestEntry> indexes =
                old == null
                        ? Collections.emptyList()
                        : Collections.singletonList(index(DELETE, old));
        List<SimpleFileEntry> a = Arrays.asList(file("X", DELETE), file("YA", ADD));
        List<SimpleFileEntry> b = Arrays.asList(file("X", DELETE), file("YB", ADD));
        assertThat(check(detection(old, true), files("X"), a, indexes)).isEmpty();
        assertThat(check(detection(old, true), files("X"), b, indexes)).isEmpty();
        assertFileConflict(
                check(detection(null, true), files("YA"), b, indexes), "Trying to delete file X");
    }

    @Test
    void testReferencesAfterApplyingOwnChanges() {
        IndexFileMeta old = dv("D0", "X");
        List<SimpleFileEntry> rewrite = Arrays.asList(file("X", DELETE), file("Y", ADD));
        assertThat(
                        check(
                                detection(old),
                                files("X"),
                                rewrite,
                                Arrays.asList(index(DELETE, old), index(ADD, dv("D1", "Y")))))
                .isEmpty();
        assertConflict(
                check(
                        detection(old),
                        files("X"),
                        rewrite,
                        Arrays.asList(index(DELETE, old), index(ADD, dv("D1", "X")))),
                "DV references data file X which is not present after applying changes");
    }

    @Test
    void testReferencesMustMatchBucket() {
        assertConflict(
                check(
                        detection(null),
                        Collections.singletonList(file("X", ADD, EMPTY_ROW, 1, 0)),
                        Collections.emptyList(),
                        Collections.singletonList(index(ADD, dv("D1", "X")))),
                "DV references data file X which is not present after applying changes");
    }

    @Test
    void testMissingReferenceMetadataIsRejected() {
        IndexFileMeta missing = new IndexFileMeta(DELETION_VECTORS_INDEX, "D1", 1, 1, null, null);
        assertConflict(
                check(
                        detection(null),
                        files("X"),
                        Collections.emptyList(),
                        Collections.singletonList(index(ADD, missing))),
                "Missing data-file references for DV file D1");
        // Pure data-file additions cannot invalidate retained DV references.
        assertThat(
                        check(
                                detection(missing),
                                files("X"),
                                Collections.singletonList(file("Y", ADD)),
                                Collections.emptyList()))
                .isEmpty();
        assertConflict(
                check(
                        detection(missing),
                        files("X"),
                        Arrays.asList(file("X", DELETE), file("Y", ADD)),
                        Collections.emptyList()),
                "Missing data-file references for DV file D1");
    }

    private ConflictDetection detection(@Nullable IndexFileMeta current) {
        return detection(current, false);
    }

    private ConflictDetection detection(@Nullable IndexFileMeta current, boolean primaryKey) {
        IndexFileHandler handler = mock(IndexFileHandler.class);
        when(snapshot.commitUser()).thenReturn("base");
        when(handler.scanBuckets(eq(snapshot), eq(DELETION_VECTORS_INDEX), anySet()))
                .thenReturn(
                        Collections.singletonMap(
                                Pair.of(EMPTY_ROW, 0),
                                current == null
                                        ? Collections.emptyList()
                                        : Collections.singletonList(current)));
        if (primaryKey) {
            return new PrimaryKeyConflictDetection(
                    "test",
                    "writer",
                    RowType.of(),
                    mock(FileStorePathFactory.class),
                    (left, right) -> 0,
                    BucketMode.HASH_FIXED,
                    true,
                    false,
                    handler,
                    null);
        }
        return new AppendConflictDetection(
                "test",
                "writer",
                RowType.of(),
                mock(FileStorePathFactory.class),
                BucketMode.HASH_FIXED,
                true,
                handler,
                null);
    }

    private Optional<RuntimeException> check(
            ConflictDetection detection,
            List<SimpleFileEntry> base,
            List<SimpleFileEntry> delta,
            List<IndexManifestEntry> indexes) {
        return check(detection, base, delta, indexes, Snapshot.CommitKind.COMPACT);
    }

    private Optional<RuntimeException> check(
            ConflictDetection detection,
            List<SimpleFileEntry> base,
            List<SimpleFileEntry> delta,
            List<IndexManifestEntry> indexes,
            Snapshot.CommitKind kind) {
        return detection.checkConflicts(snapshot, base, delta, indexes, null, kind);
    }

    private void assertConflict(Optional<RuntimeException> result, String detail) {
        assertThat(result).isPresent();
        assertThat(result.get())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Deletion vector conflict")
                .hasMessageContaining(detail);
    }

    private void assertFileConflict(Optional<RuntimeException> result, String detail) {
        assertThat(result).isPresent();
        assertThat(result.get())
                .hasMessageContaining("File deletion conflicts detected")
                .hasStackTraceContaining(detail);
    }

    private static IndexFileMeta dv(String name, String... files) {
        LinkedHashMap<String, DeletionVectorMeta> ranges = new LinkedHashMap<>();
        for (String file : files) {
            ranges.put(file, new DeletionVectorMeta(file, 1, 1, 1L));
        }
        return new IndexFileMeta(DELETION_VECTORS_INDEX, name, 1, files.length, ranges, null);
    }

    private static IndexManifestEntry index(FileKind kind, IndexFileMeta meta) {
        return new IndexManifestEntry(kind, EMPTY_ROW, 0, meta);
    }

    private static List<SimpleFileEntry> files(String... names) {
        return Arrays.stream(names)
                .map(name -> file(name, ADD))
                .collect(java.util.stream.Collectors.toList());
    }

    private static SimpleFileEntry file(String name, FileKind kind) {
        return file(name, kind, EMPTY_ROW, 0, 0);
    }

    private static SimpleFileEntry file(
            String name, FileKind kind, BinaryRow partition, int bucket, int level) {
        return new SimpleFileEntry(
                kind,
                partition,
                bucket,
                2,
                level,
                name,
                Collections.emptyList(),
                null,
                EMPTY_ROW,
                EMPTY_ROW,
                null,
                1,
                null);
    }
}
