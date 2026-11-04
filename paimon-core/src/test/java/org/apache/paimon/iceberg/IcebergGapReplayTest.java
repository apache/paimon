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

package org.apache.paimon.iceberg;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for replaying missing Iceberg metadata versions. Each test leaves a gap by committing some
 * snapshots with Iceberg metadata turned off, then lets the Iceberg callback run for the newest
 * snapshot, the way a retry does.
 */
public class IcebergGapReplayTest {

    @TempDir java.nio.file.Path tempDir;

    // ------------------------------------------------------------------------
    //  Gap replay
    //
    //  Every test is a story on one table. A gap is left by committing snapshots with
    //  Iceberg metadata turned off, then the Iceberg callback runs for the newest snapshot,
    //  the way a retry does, and the next part of the story starts from what is left.
    // ------------------------------------------------------------------------

    private static final ReplayAction NOTHING = (point, value) -> {};

    @Test
    public void testGapReplay() throws Exception {
        ReplayTable t = new ReplayTable(Collections.emptyMap());
        t.append(t.enabled, 1);
        t.append(t.enabled, 2);
        t.append(t.enabled, 3);
        // every kind of change in one gap: an append, an overwrite that leaves the same number
        // of rows in none of the old files, more appends and a compaction of the three files
        t.append(t.disabled, 4);
        t.overwrite(t.disabled, 11, 12, 13, 14);
        t.append(t.disabled, 5);
        t.append(t.disabled, 6);
        t.compact(t.disabled);
        assertThat(t.disabled.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        long n = t.latest();
        assertThat(n).isEqualTo(8);
        assertThat(versionFiles(t.enabled))
                .containsExactlyInAnyOrder(
                        "v1.metadata.json", "v2.metadata.json", "v3.metadata.json");

        IcebergMetadata base = readMetadata(t.enabled, 3);
        int baseManifests = manifestNames(t.enabled, 3).size();
        Set<String> before = metadataFileNames(t.enabled);
        List<String> events = new ArrayList<>();
        List<Set<String>> versionsAtSteps = new ArrayList<>();
        t.replay(
                t.identifier,
                events,
                (point, value) -> {
                    if (point == IcebergCommitCallback.ReplayPoint.STEP_DONE) {
                        versionsAtSteps.add(versionFiles(t.enabled));
                    }
                });

        assertThat(events)
                .containsSubsequence(
                        "BASE_SELECTED:3",
                        "LISTS_READ:3",
                        "COPIED:" + baseManifests,
                        "STEP_DONE:4",
                        "STEP_DONE:5",
                        "STEP_DONE:6",
                        "STEP_DONE:7",
                        "STEP_DONE:8",
                        "BEFORE_WRITE:8",
                        "EXPOSED:8");
        assertThat(events).noneMatch(event -> event.startsWith("REBUILD"));
        // no intermediate version is visible while the steps run
        assertThat(versionsAtSteps).hasSize(5);
        for (Set<String> versions : versionsAtSteps) {
            assertThat(versions)
                    .containsExactlyInAnyOrder(
                            "v1.metadata.json", "v2.metadata.json", "v3.metadata.json");
        }

        IcebergMetadata result = readMetadata(t.enabled, n);
        assertThat(result.tableUuid()).isEqualTo(base.tableUuid());
        assertThat(result.snapshots().stream().map(IcebergSnapshot::snapshotId))
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        assertThat(readVersionHint(t.enabled)).isEqualTo(n);

        // every retained snapshot reads files of its own, and the replay deletes nothing
        Set<String> reachable = filesReachableFrom(t.enabled, n);
        assertThat(reachable).doesNotContainAnyElementsOf(before);
        Set<String> after = metadataFileNames(t.enabled);
        assertThat(after).containsAll(before);
        assertThat(avroFiles(after)).isEqualTo(union(avroFiles(before), reachable));

        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, n));
        assertThat(icebergFilePaths(null))
                .containsExactlyInAnyOrderElementsOf(paimonFilePaths(t.enabled, n));

        // the originals are not needed any more
        Path metadataDirectory = IcebergCommitCallback.catalogTableMetadataPath(t.enabled);
        for (String name : avroFiles(before)) {
            t.enabled.fileIO().delete(new Path(metadataDirectory, name), false);
        }
        for (long id = 1; id <= n; id++) {
            long snapshotId = id;
            assertThat(
                            getIcebergResult(
                                    icebergTable ->
                                            IcebergGenerics.read(icebergTable)
                                                    .useSnapshot(snapshotId)
                                                    .build(),
                                    Record::toString))
                    .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, id));
        }

        // the replayed version is a base like any other: the next commit extends it
        t.append(t.enabled, 20);
        assertThat(readVersionHint(t.enabled)).isEqualTo(n + 1);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, n + 1));

        // a gap over a table that keeps only the newest snapshots: the base is needed to
        // replay, and retention leaves it out of the result
        t.gap(21, 22, 23, 24);
        long m = t.latest();
        assertThat(m).isEqualTo(13);
        IcebergMetadata head = readMetadata(t.enabled, n + 1);
        List<String> headLists = new ArrayList<>();
        for (IcebergSnapshot snapshot : head.snapshots()) {
            headLists.add(new Path(snapshot.manifestList()).getName());
        }
        Set<String> headManifests = listedBy(t.enabled, head.currentSnapshot());
        Set<String> beforeRetention = metadataFileNames(t.enabled);
        Map<String, String> retention = new HashMap<>();
        retention.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "2");
        retention.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "3");

        events = new ArrayList<>();
        t.replay(retention, t.identifier, events, NOTHING);

        assertThat(events)
                .containsSubsequence(
                        "BASE_SELECTED:9", "STEP_DONE:10", "STEP_DONE:13", "EXPOSED:13");
        assertThat(readMetadata(t.enabled, m).snapshots().stream().map(IcebergSnapshot::snapshotId))
                .containsExactly(m - 2, m - 1, m);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, m));
        // the copies of the base are gone, nothing else is left behind
        assertThat(difference(avroFiles(metadataFileNames(t.enabled)), avroFiles(beforeRetention)))
                .isEqualTo(filesReachableFrom(t.enabled, m));
        // retention drops the lists of the dropped snapshots, as it does for every commit, and
        // the manifests that the result still uses stay
        IcebergPathFactory pathFactory = new IcebergPathFactory(metadataDirectory);
        for (String list : headLists) {
            assertThat(t.enabled.fileIO().exists(pathFactory.toManifestListPath(list))).isFalse();
        }
        for (String manifest : headManifests) {
            assertThat(t.enabled.fileIO().exists(pathFactory.toManifestFilePath(manifest)))
                    .isTrue();
        }
    }

    @Test
    public void testReplayChoosesAnUnbrokenBase() throws Exception {
        ReplayTable t = new ReplayTable(Collections.emptyMap());
        for (int key = 1; key <= 6; key++) {
            t.append(t.enabled, key);
        }
        t.gap(7, 8);
        long n = t.latest();
        assertThat(n).isEqualTo(8);

        // the four newest versions are unusable, each in its own way
        setIdentity(t.enabled, 6, null);
        setIdentity(t.enabled, 5, "a-snapshot-of-another-timeline");
        // a file that was cut short
        t.enabled
                .fileIO()
                .overwriteFileUtf8(
                        metadataPath(t.enabled, 4), "{\"format-version\": 2, \"snapshots\": [");
        // version 3 lists a manifest that is gone, which shows only when its history is copied
        t.enabled.fileIO().delete(manifestCreatedAt(t.enabled, 3, 3), false);
        Set<String> before = metadataFileNames(t.enabled);

        List<String> events = new ArrayList<>();
        t.replay(t.identifier, events, NOTHING);

        // nothing is rebuilt: the one reselection finds version 2
        assertThat(events)
                .containsSubsequence(
                        "CANDIDATE_REJECTED:6",
                        "CANDIDATE_REJECTED:5",
                        "CANDIDATE_REJECTED:4",
                        "BASE_SELECTED:3",
                        "LISTS_READ:3",
                        "CANDIDATE_REJECTED:3",
                        "BASE_SELECTED:2",
                        "LISTS_READ:2",
                        "STEP_DONE:3",
                        "STEP_DONE:8",
                        "EXPOSED:8");
        assertThat(events).noneMatch(event -> event.startsWith("REBUILD"));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, n));
        // what the rejected attempt copied is gone
        assertThat(difference(avroFiles(metadataFileNames(t.enabled)), avroFiles(before)))
                .isSubsetOf(filesReachableFrom(t.enabled, n));

        // version 8 loses a manifest as well. Only one reselection is made, and it meets a
        // version that lists the manifest that is gone, so the head is rebuilt. The rebuild
        // cleans up the older versions, and the one that was cut short does not stop it
        t.gap(9, 10);
        long m = t.latest();
        assertThat(m).isEqualTo(10);
        t.enabled.fileIO().delete(manifestCreatedAt(t.enabled, 8, 1), false);
        before = metadataFileNames(t.enabled);

        events = new ArrayList<>();
        t.replay(t.identifier, events, NOTHING);

        assertThat(events)
                .containsExactly(
                        "BASE_SELECTED:8",
                        "LISTS_READ:8",
                        "CANDIDATE_REJECTED:8",
                        "CANDIDATE_REJECTED:6",
                        "CANDIDATE_REJECTED:5",
                        "CANDIDATE_REJECTED:4",
                        "BASE_SELECTED:3",
                        "LISTS_READ:3",
                        "CANDIDATE_REJECTED:3",
                        "REBUILD:10");
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, m));
        assertThat(difference(avroFiles(metadataFileNames(t.enabled)), avroFiles(before)))
                .isSubsetOf(filesReachableFrom(t.enabled, m));
    }

    @Test
    public void testReplayFallsBackToRebuild() throws Exception {
        ReplayTable t = new ReplayTable(Collections.emptyMap());
        t.append(t.enabled, 1);
        t.append(t.enabled, 2);
        t.append(t.enabled, 3);
        int key = 10;

        // the base has the right identity and a list that lost a manifest, so its row count
        // does not match Paimon
        t.gap(key++, key++);
        long n = t.latest();
        IcebergMetadata base = readMetadata(t.enabled, 3);
        dropLastManifest(t.enabled, 3);
        int baseManifests = manifestNames(t.enabled, 3).size();
        Set<String> before = metadataFileNames(t.enabled);
        List<String> events = new ArrayList<>();
        t.replay(t.identifier, events, NOTHING);
        assertThat(events)
                .containsExactly(
                        "BASE_SELECTED:3",
                        "LISTS_READ:3",
                        "COPIED:" + baseManifests,
                        "REBUILD:" + n);
        IcebergMetadata rebuilt = readMetadata(t.enabled, n);
        assertThat(rebuilt.tableUuid()).isEqualTo(base.tableUuid());
        assertThat(rebuilt.lastColumnId()).isGreaterThanOrEqualTo(base.lastColumnId());
        assertRebuilt(t, n, events, before);

        // the default strategy does not replay at all
        t.gap(key++, key++);
        n = t.latest();
        before = metadataFileNames(t.enabled);
        events = new ArrayList<>();
        t.replay(
                Collections.singletonMap(
                        IcebergOptions.METADATA_MISSING_BASE_STRATEGY.key(), "rebuild"),
                t.identifier,
                events,
                NOTHING);
        assertThat(events).containsExactly("REBUILD:" + n);
        assertRebuilt(t, n, events, before);

        // every limit sends the replay to the rebuild
        for (String budget :
                new String[] {
                    "max-snapshots=1",
                    "max-delta-entries=1",
                    "max-base-manifest-bytes=1 b",
                    "max-duration=0 ms"
                }) {
            String[] parts = budget.split("=");
            t.gap(key++, key++);
            n = t.latest();
            before = metadataFileNames(t.enabled);
            events = new ArrayList<>();
            t.replay(
                    Collections.singletonMap("metadata.iceberg.replay." + parts[0], parts[1]),
                    t.identifier,
                    events,
                    NOTHING);
            assertRebuilt(t, n, events, before);
        }

        // a delta that is missing before the replay starts, and one that goes missing after
        // the history was copied, which the preflight cannot see
        for (boolean afterCopy : new boolean[] {false, true}) {
            t.gap(key++, key++);
            n = t.latest();
            long prev = n - 2;
            Path delta =
                    t.enabled
                            .store()
                            .pathFactory()
                            .toManifestListPath(
                                    t.enabled
                                            .snapshotManager()
                                            .snapshot(prev + 1)
                                            .deltaManifestList());
            if (!afterCopy) {
                t.enabled.fileIO().delete(delta, false);
            }
            before = metadataFileNames(t.enabled);
            events = new ArrayList<>();
            AtomicInteger copied = new AtomicInteger();
            t.replay(
                    t.identifier,
                    events,
                    (point, value) -> {
                        if (afterCopy
                                && point == IcebergCommitCallback.ReplayPoint.COPIED
                                && copied.getAndIncrement() == 0) {
                            t.enabled.fileIO().delete(delta, false);
                        }
                    });
            assertRebuilt(t, n, events, before);
        }

        // a table with a primary key is not replayed
        ReplayTable keyed =
                new ReplayTable(
                        Collections.singletonList("k"),
                        Collections.emptyMap(),
                        LocalFileIO.create(),
                        "keyed");
        keyed.append(keyed.enabled, 1);
        keyed.append(keyed.enabled, 2);
        keyed.gap(3, 4);
        events = new ArrayList<>();
        keyed.replay(keyed.identifier, events, NOTHING);
        assertThat(events).containsExactly("REBUILD:" + keyed.latest());
        assertThat(versionFiles(keyed.enabled)).contains("v" + keyed.latest() + ".metadata.json");

        // the duration is also checked while the manifests of one snapshot are copied. Only the
        // working base is kept, and its list holds one manifest for each commit
        FaultyFileIO slowIO = new FaultyFileIO();
        ReplayTable slow =
                new ReplayTable(Collections.emptyList(), Collections.emptyMap(), slowIO, "slow");
        slow.append(slow.enabled, 1);
        slow.append(slow.enabled, 2);
        slow.append(slow.enabled, 3);
        int manifests = manifestNames(slow.enabled, 3).size();
        assertThat(manifests).isGreaterThanOrEqualTo(3);
        slow.gap(4, 5);
        slowIO.copyDelayMillis = 250;
        Map<String, String> slowOptions = new HashMap<>();
        slowOptions.put(IcebergOptions.REPLAY_MAX_DURATION.key(), "300 ms");
        slowOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
        slowOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "3");
        events = new ArrayList<>();
        slow.replay(slowOptions, slow.identifier, events, NOTHING);
        assertThat(events).noneMatch(event -> event.startsWith("COPIED"));
        assertThat(events).contains("REBUILD:" + slow.latest());
        assertThat(slowIO.copies.get()).isLessThan(manifests);
    }

    /** The head was rebuilt once, from the right data, and nothing of the attempt is left. */
    private void assertRebuilt(ReplayTable t, long n, List<String> events, Set<String> before)
            throws Exception {
        assertThat(events).filteredOn(event -> event.startsWith("REBUILD")).hasSize(1);
        assertThat(events).contains("REBUILD:" + n);
        assertThat(events).noneMatch(event -> event.startsWith("STEP_DONE"));
        assertThat(events).noneMatch(event -> event.startsWith("EXPOSED"));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, n));
        assertThat(difference(avroFiles(metadataFileNames(t.enabled)), avroFiles(before)))
                .isSubsetOf(filesReachableFrom(t.enabled, n));
    }

    @Test
    public void testReplayDuringConcurrentChange() throws Exception {
        ReplayTable t = new ReplayTable(Collections.emptyMap());
        t.append(t.enabled, 1);
        t.append(t.enabled, 2);
        t.append(t.enabled, 3);

        // the head moves while the replay runs: its metadata is a version that a later commit
        // can extend, and no hint or catalog pointer moves behind the head
        t.gap(4, 5, 6);
        long n = t.latest();
        long identifier = t.identifier;
        Set<String> before = metadataFileNames(t.enabled);
        List<String> events = new ArrayList<>();
        interruptOnce(
                t,
                identifier,
                events,
                IcebergCommitCallback.ReplayPoint.COPIED,
                -1,
                () -> t.append(t.disabled, 7));
        assertThat(events).contains("EXPOSED:" + n);
        assertThat(versionFiles(t.enabled)).contains("v" + n + ".metadata.json");
        assertThat(readVersionHint(t.enabled)).isEqualTo(3);
        assertThat(difference(avroFiles(metadataFileNames(t.enabled)), avroFiles(before)))
                .isEqualTo(filesReachableFrom(t.enabled, n));
        IcebergCommitCallback next = new IcebergCommitCallback(t.enabled, t.user);
        next.retry(new ManifestCommittable(t.identifier));
        next.close();
        assertThat(readVersionHint(t.enabled)).isEqualTo(n + 1);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, n + 1));

        // snapshots 9 and 10 are written again by other commits between two steps
        t.gap(8, 9, 10);
        n = t.latest();
        assertThat(n).isEqualTo(10);
        before = metadataFileNames(t.enabled);
        events = new ArrayList<>();
        interruptOnce(
                t,
                t.identifier,
                events,
                IcebergCommitCallback.ReplayPoint.STEP_DONE,
                8,
                () -> {
                    t.enabled.rollbackTo(8);
                    t.append(t.disabled, 15);
                    t.append(t.disabled, 16);
                });
        assertAbandoned(t, n, events, before);

        // the last snapshot is written again after every step is done
        t.gap(17, 18);
        n = t.latest();
        assertThat(n).isEqualTo(12);
        before = metadataFileNames(t.enabled);
        events = new ArrayList<>();
        long last = n;
        interruptOnce(
                t,
                t.identifier,
                events,
                IcebergCommitCallback.ReplayPoint.STEP_DONE,
                last,
                () -> {
                    t.enabled.rollbackTo(last - 1);
                    t.append(t.disabled, 19);
                });
        assertAbandoned(t, n, events, before);

        // another writer publishes the same snapshot first: its metadata is the one that stays,
        // and it owns its files
        t.gap(20, 21);
        n = t.latest();
        long twinIdentifier = t.identifier;
        before = metadataFileNames(t.enabled);
        events = new ArrayList<>();
        interruptOnce(
                t,
                twinIdentifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_WRITE,
                -1,
                () -> {
                    IcebergCommitCallback twin = new IcebergCommitCallback(t.enabled, t.user);
                    twin.retry(new ManifestCommittable(twinIdentifier));
                    twin.close();
                });
        assertThat(events).contains("EXPOSED:" + n);
        assertThat(readVersionHint(t.enabled)).isEqualTo(n);
        assertThat(difference(avroFiles(metadataFileNames(t.enabled)), avroFiles(before)))
                .isEqualTo(filesReachableFrom(t.enabled, n));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, n));

        // a limit is hit before any step checked the source, and the head is another commit with
        // the id of this snapshot, so nothing is rebuilt
        t.gap(22, 23);
        n = t.latest();
        long replacedId = n;
        long replacedIdentifier = t.identifier;
        before = metadataFileNames(t.enabled);
        events = new ArrayList<>();
        interruptOnce(
                t,
                Collections.singletonMap(IcebergOptions.REPLAY_MAX_DELTA_ENTRIES.key(), "1"),
                replacedIdentifier,
                events,
                IcebergCommitCallback.ReplayPoint.BASE_SELECTED,
                -1,
                () -> {
                    t.enabled.rollbackTo(replacedId - 1);
                    t.append(t.disabled, 24);
                });
        assertAbandoned(t, n, events, before);

        // a delta goes missing and the head has moved, so nothing is rebuilt for a snapshot
        // that is not the head
        t.gap(25, 26);
        n = t.latest();
        long gapIdentifier = t.identifier;
        Path delta =
                t.enabled
                        .store()
                        .pathFactory()
                        .toManifestListPath(
                                t.enabled.snapshotManager().snapshot(n - 1).deltaManifestList());
        before = metadataFileNames(t.enabled);
        events = new ArrayList<>();
        interruptOnce(
                t,
                gapIdentifier,
                events,
                IcebergCommitCallback.ReplayPoint.COPIED,
                -1,
                () -> {
                    t.enabled.fileIO().delete(delta, false);
                    t.append(t.disabled, 27);
                });
        assertAbandoned(t, n, events, before);
    }

    /** Runs the replay and, the first time it reaches a point, does something else first. */
    private void interruptOnce(
            ReplayTable t,
            long commitIdentifier,
            List<String> events,
            IcebergCommitCallback.ReplayPoint at,
            long atValue,
            ReplayInterruption interruption)
            throws Exception {
        interruptOnce(
                t, Collections.emptyMap(), commitIdentifier, events, at, atValue, interruption);
    }

    private void interruptOnce(
            ReplayTable t,
            Map<String, String> options,
            long commitIdentifier,
            List<String> events,
            IcebergCommitCallback.ReplayPoint at,
            long atValue,
            ReplayInterruption interruption)
            throws Exception {
        AtomicInteger interrupted = new AtomicInteger();
        t.replay(
                options,
                commitIdentifier,
                events,
                (point, value) -> {
                    if (point == at
                            && (atValue < 0 || value == atValue)
                            && interrupted.getAndIncrement() == 0) {
                        interruption.run();
                    }
                });
    }

    private interface ReplayInterruption {
        void run() throws Exception;
    }

    /** The replay was dropped: no rebuild, no metadata for the snapshot, no file left. */
    private void assertAbandoned(ReplayTable t, long n, List<String> events, Set<String> before)
            throws Exception {
        assertThat(events).contains("ABANDONED:" + n);
        assertThat(events).noneMatch(event -> event.startsWith("REBUILD"));
        assertThat(events).noneMatch(event -> event.startsWith("EXPOSED"));
        assertThat(versionFiles(t.enabled)).doesNotContain("v" + n + ".metadata.json");
        assertThat(avroFiles(metadataFileNames(t.enabled))).isEqualTo(avroFiles(before));
    }

    private enum Failure {
        BASE_METADATA_UNREADABLE,
        BASE_LIST_UNREADABLE,
        DELTA_LIST_UNREADABLE,
        BEFORE_WRITE,
        AFTER_WRITE,
        CATALOG
    }

    @Test
    public void testReplayFailureStates() throws Exception {
        RecordingIcebergMetadataCommitter.COMMITS.clear();
        RecordingIcebergMetadataCommitter.BASES.clear();
        RecordingIcebergMetadataCommitter.failNextCommit = false;
        FaultyFileIO fileIO = new FaultyFileIO();
        ReplayTable t =
                new ReplayTable(
                        Collections.emptyList(),
                        Collections.singletonMap(
                                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                                IcebergOptions.StorageType.HADOOP_CATALOG.toString()),
                        fileIO,
                        "mydb");
        t.append(t.enabled, 1);
        t.append(t.enabled, 2);
        t.append(t.enabled, 3);

        int key = 10;
        for (Failure failure : Failure.values()) {
            long prev = t.latest();
            t.gap(key++, key++);
            long n = t.latest();
            long identifier = t.identifier;
            Set<String> before = metadataFileNames(t.enabled);
            RecordingIcebergMetadataCommitter.COMMITS.clear();
            RecordingIcebergMetadataCommitter.BASES.clear();

            // the base and the delta are named before the storage fails. A stream that closed
            // early reads like a file that is short, so the lists fail that way
            switch (failure) {
                case BASE_METADATA_UNREADABLE:
                    fileIO.failReadsOf(
                            "v" + prev + ".metadata.json",
                            new IOException("injected storage failure"));
                    break;
                case BASE_LIST_UNREADABLE:
                    fileIO.failReadsOf(
                            new Path(readMetadata(t.enabled, prev).currentSnapshot().manifestList())
                                    .getName(),
                            new EOFException("injected storage failure"));
                    break;
                case DELTA_LIST_UNREADABLE:
                    fileIO.failReadsOf(
                            t.enabled.snapshotManager().snapshot(prev + 1).deltaManifestList(),
                            new EOFException("injected storage failure"));
                    break;
                case CATALOG:
                    RecordingIcebergMetadataCommitter.failNextCommit = true;
                    break;
                default:
            }
            List<String> events = new ArrayList<>();
            assertThatThrownBy(
                            () ->
                                    t.replay(
                                            identifier,
                                            events,
                                            (point, value) -> {
                                                if (failure == Failure.BEFORE_WRITE
                                                        && point
                                                                == IcebergCommitCallback.ReplayPoint
                                                                        .BEFORE_WRITE) {
                                                    throw new IllegalStateException("injected");
                                                }
                                                if (failure == Failure.AFTER_WRITE
                                                        && point
                                                                == IcebergCommitCallback.ReplayPoint
                                                                        .EXPOSED) {
                                                    throw new IllegalStateException("injected");
                                                }
                                            }))
                    .as(failure.toString())
                    .hasStackTraceContaining("injected");
            fileIO.stopFailing();
            RecordingIcebergMetadataCommitter.failNextCommit = false;

            // a failure is no reason to read the whole table
            assertThat(events).noneMatch(event -> event.startsWith("REBUILD"));
            assertThat(RecordingIcebergMetadataCommitter.COMMITS).isEmpty();
            Set<String> failed = metadataFileNames(t.enabled);
            switch (failure) {
                case BEFORE_WRITE:
                case BASE_METADATA_UNREADABLE:
                case BASE_LIST_UNREADABLE:
                case DELTA_LIST_UNREADABLE:
                    // nothing was exposed, so nothing is left
                    assertThat(versionFiles(t.enabled))
                            .as(failure.toString())
                            .doesNotContain("v" + n + ".metadata.json");
                    assertThat(failed).as(failure.toString()).isEqualTo(before);
                    assertThat(readVersionHint(t.enabled)).isEqualTo(prev);
                    break;
                default:
                    // the metadata is durable, so every file it reaches stays
                    assertThat(versionFiles(t.enabled)).contains("v" + n + ".metadata.json");
                    assertThat(failed).containsAll(filesReachableFrom(t.enabled, n));
                    assertThat(difference(avroFiles(failed), avroFiles(before)))
                            .isEqualTo(filesReachableFrom(t.enabled, n));
                    assertThat(readVersionHint(t.enabled))
                            .isEqualTo(failure == Failure.CATALOG ? n : prev);
            }

            // the retry finishes the job, publishing like a rebuild does
            List<String> retryEvents = new ArrayList<>();
            t.replay(identifier, retryEvents, NOTHING);
            assertThat(retryEvents).noneMatch(event -> event.startsWith("REBUILD"));
            assertThat(readVersionHint(t.enabled)).isEqualTo(n);
            assertThat(RecordingIcebergMetadataCommitter.COMMITS).hasSize(1);
            assertThat(RecordingIcebergMetadataCommitter.BASES.get(0)).isNull();
            assertThat(metadataFileNames(t.enabled)).containsAll(filesReachableFrom(t.enabled, n));
        }
    }

    /**
     * A file system that fails every read of one file, to look like a storage failure, and that can
     * make copies slow.
     */
    private static class FaultyFileIO extends LocalFileIO {

        private volatile String failingName;
        private volatile IOException failure;
        private volatile long copyDelayMillis;
        private final AtomicInteger copies = new AtomicInteger();

        private void failReadsOf(String name, IOException failure) {
            this.failingName = name;
            this.failure = failure;
        }

        private void stopFailing() {
            failReadsOf(null, null);
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            if (path.getName().equals(failingName)) {
                throw failure;
            }
            return super.newInputStream(path);
        }

        @Override
        public void copyFile(Path sourcePath, Path targetPath, boolean overwrite)
                throws IOException {
            copies.incrementAndGet();
            try {
                Thread.sleep(copyDelayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            super.copyFile(sourcePath, targetPath, overwrite);
        }
    }

    /** A table with Iceberg metadata on, and the same table with it off, to leave gaps. */
    private final class ReplayTable {

        private final FileStoreTable enabled;
        private final FileStoreTable disabled;
        private final String user = UUID.randomUUID().toString();
        private long identifier = 0;

        private ReplayTable(Map<String, String> options) throws Exception {
            this(Collections.emptyList(), options, LocalFileIO.create(), "mydb");
        }

        private ReplayTable(
                List<String> primaryKeys,
                Map<String, String> options,
                FileIO fileIO,
                String database)
                throws Exception {
            Map<String, String> created = new HashMap<>();
            // compaction happens only when a test asks for it
            created.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "100");
            if (primaryKeys.isEmpty()) {
                created.put(CoreOptions.BUCKET_KEY.key(), "k");
            }
            FileStoreTable table =
                    createPaimonTable(
                            fileIO,
                            database,
                            RowType.of(
                                    new DataType[] {DataTypes.INT(), DataTypes.INT()},
                                    new String[] {"k", "v"}),
                            Collections.emptyList(),
                            primaryKeys,
                            1,
                            created);
            Map<String, String> all = new HashMap<>();
            all.put(IcebergOptions.METADATA_MISSING_BASE_STRATEGY.key(), "replay");
            all.put(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX.key(), "100");
            all.putAll(options);
            this.enabled = table.copy(all);
            // Paimon keeps every snapshot while the gap commits run, whatever the tight
            // retention of the Iceberg metadata says
            Map<String, String> gap = new HashMap<>();
            gap.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled");
            gap.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "100");
            gap.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), String.valueOf(Integer.MAX_VALUE));
            this.disabled = enabled.copy(gap);
        }

        private void append(FileStoreTable via, int... keys) throws Exception {
            commit(via, false, false, keys);
        }

        private void overwrite(FileStoreTable via, int... keys) throws Exception {
            commit(via, true, false, keys);
        }

        private void compact(FileStoreTable via) throws Exception {
            commit(via, false, true);
        }

        private void commit(FileStoreTable via, boolean overwrite, boolean compact, int... keys)
                throws Exception {
            identifier++;
            try (TableWriteImpl<?> write =
                            via.newWrite(user)
                                    .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
                    TableCommitImpl commit = via.newCommit(user)) {
                if (overwrite) {
                    commit.withOverwrite(Collections.emptyMap());
                }
                for (int key : keys) {
                    write.write(GenericRow.of(key, key * 10));
                }
                if (compact) {
                    write.compact(BinaryRow.EMPTY_ROW, 0, true);
                }
                commit.commit(identifier, write.prepareCommit(compact, identifier));
            }
        }

        /** Commits one snapshot for each key without Iceberg metadata, so a gap is left. */
        private void gap(int... keys) throws Exception {
            for (int key : keys) {
                append(disabled, key);
            }
        }

        private long latest() {
            return enabled.snapshotManager().latestSnapshotId();
        }

        /** Runs the Iceberg callback for a commit, like a retry does. */
        private void replay(long commitIdentifier, List<String> events, ReplayAction action)
                throws Exception {
            replay(Collections.emptyMap(), commitIdentifier, events, action);
        }

        /** The same, with other table options than the table was created with. */
        private void replay(
                Map<String, String> options,
                long commitIdentifier,
                List<String> events,
                ReplayAction action)
                throws Exception {
            IcebergCommitCallback callback =
                    new IcebergCommitCallback(
                            options.isEmpty() ? enabled : enabled.copy(options), user);
            callback.setReplayHook(
                    (point, value) -> {
                        events.add(point + ":" + value);
                        try {
                            action.at(point, value);
                        } catch (RuntimeException e) {
                            throw e;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            try {
                callback.retry(new ManifestCommittable(commitIdentifier));
            } finally {
                callback.close();
            }
        }
    }

    private interface ReplayAction {
        void at(IcebergCommitCallback.ReplayPoint point, long value) throws Exception;
    }

    private IcebergMetadata readMetadata(FileStoreTable table, long version) {
        return IcebergMetadata.fromPath(
                table.fileIO(),
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table))
                        .toMetadataPath(version));
    }

    private Path metadataPath(FileStoreTable table, long version) {
        return new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table))
                .toMetadataPath(version);
    }

    /** The manifests that the list of one snapshot lists. */
    private Set<String> listedBy(FileStoreTable table, IcebergSnapshot snapshot) {
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        IcebergManifestList manifestList = IcebergManifestList.create(table, pathFactory);
        Set<String> names = new HashSet<>();
        for (IcebergManifestFileMeta meta :
                manifestList.read(new Path(snapshot.manifestList()).getName())) {
            names.add(new Path(meta.manifestPath()).getName());
        }
        return names;
    }

    private long readVersionHint(FileStoreTable table) throws IOException {
        return Long.parseLong(
                table.fileIO()
                        .readFileUtf8(
                                new Path(
                                        IcebergCommitCallback.catalogTableMetadataPath(table),
                                        "version-hint.text"))
                        .trim());
    }

    private Set<String> metadataFileNames(FileStoreTable table) throws IOException {
        Set<String> names = new HashSet<>();
        for (FileStatus status :
                table.fileIO().listStatus(IcebergCommitCallback.catalogTableMetadataPath(table))) {
            names.add(status.getPath().getName());
        }
        return names;
    }

    private Set<String> versionFiles(FileStoreTable table) {
        try {
            return metadataFileNames(table).stream()
                    .filter(name -> name.matches("v\\d+\\.metadata\\.json"))
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** The manifest lists and manifests, without the metadata files. */
    private Set<String> avroFiles(Set<String> names) {
        return names.stream().filter(name -> name.endsWith(".avro")).collect(Collectors.toSet());
    }

    private Set<String> union(Set<String> first, Set<String> second) {
        Set<String> result = new HashSet<>(first);
        result.addAll(second);
        return result;
    }

    private Set<String> difference(Set<String> first, Set<String> second) {
        Set<String> result = new HashSet<>(first);
        result.removeAll(second);
        return result;
    }

    /** The manifest lists and manifests that any snapshot of a metadata version reaches. */
    private Set<String> filesReachableFrom(FileStoreTable table, long version) {
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        IcebergManifestList manifestList = IcebergManifestList.create(table, pathFactory);
        Set<String> names = new HashSet<>();
        for (IcebergSnapshot snapshot : readMetadata(table, version).snapshots()) {
            String listName = new Path(snapshot.manifestList()).getName();
            names.add(listName);
            for (IcebergManifestFileMeta meta : manifestList.read(listName)) {
                names.add(new Path(meta.manifestPath()).getName());
            }
        }
        return names;
    }

    /** The manifests that the snapshots of a metadata version list. */
    private Set<String> manifestNames(FileStoreTable table, long version) {
        Set<String> all = filesReachableFrom(table, version);
        for (IcebergSnapshot snapshot : readMetadata(table, version).snapshots()) {
            all.remove(new Path(snapshot.manifestList()).getName());
        }
        return all;
    }

    /** The manifest that was written for a snapshot, as a metadata version lists it. */
    private Path manifestCreatedAt(FileStoreTable table, long version, long sequenceNumber) {
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        IcebergManifestList manifestList = IcebergManifestList.create(table, pathFactory);
        for (IcebergSnapshot snapshot : readMetadata(table, version).snapshots()) {
            for (IcebergManifestFileMeta meta :
                    manifestList.read(new Path(snapshot.manifestList()).getName())) {
                if (meta.sequenceNumber() == sequenceNumber) {
                    return new Path(meta.manifestPath());
                }
            }
        }
        throw new IllegalStateException("No manifest of snapshot " + sequenceNumber);
    }

    /** Replaces the commit identity of the current snapshot of a metadata version. */
    private void setIdentity(FileStoreTable table, long version, String identity) throws Exception {
        Path path =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table))
                        .toMetadataPath(version);
        ObjectNode metadataJson =
                JsonSerdeUtil.asSpecificNodeType(
                        table.fileIO().readFileUtf8(path), ObjectNode.class);
        JsonNode snapshots = metadataJson.get("snapshots");
        ObjectNode summary = (ObjectNode) snapshots.get(snapshots.size() - 1).get("summary");
        if (identity == null) {
            summary.remove(IcebergCommitCallback.SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY);
        } else {
            summary.put(IcebergCommitCallback.SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY, identity);
        }
        table.fileIO().overwriteFileUtf8(path, JsonSerdeUtil.writeValueAsString(metadataJson));
    }

    /** Makes the list of the current snapshot of a metadata version lose its last manifest. */
    private void dropLastManifest(FileStoreTable table, long version) throws Exception {
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        IcebergManifestList manifestList = IcebergManifestList.create(table, pathFactory);
        IcebergMetadata metadata = readMetadata(table, version);
        IcebergSnapshot current = metadata.currentSnapshot();
        List<IcebergManifestFileMeta> metas =
                new ArrayList<>(manifestList.read(new Path(current.manifestList()).getName()));
        metas.remove(metas.size() - 1);
        String listName = manifestList.writeWithoutRolling(metas);
        List<IcebergSnapshot> snapshots = new ArrayList<>(metadata.snapshots());
        snapshots.set(
                snapshots.size() - 1,
                current.withManifestList(pathFactory.toManifestListPath(listName).toString()));
        table.fileIO()
                .overwriteFileUtf8(
                        pathFactory.toMetadataPath(version),
                        metadata.withSnapshots(snapshots).toJson());
    }

    /** The rows of a Paimon snapshot, the way Iceberg prints them. */
    private List<String> paimonRows(FileStoreTable table, long snapshotId) throws Exception {
        FileStoreTable pinned =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshotId)));
        ReadBuilder readBuilder = pinned.newReadBuilder();
        List<String> rows = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row -> rows.add("Record(" + row.getInt(0) + ", " + row.getInt(1) + ")"));
        return rows;
    }

    /** The data files a Paimon snapshot is made of. */
    private List<String> paimonFilePaths(FileStoreTable table, long snapshotId) {
        List<String> paths = new ArrayList<>();
        for (DataSplit split :
                table.newSnapshotReader().withSnapshot(snapshotId).read().dataSplits()) {
            for (RawFile file : split.convertToRawFiles().get()) {
                paths.add(file.path());
            }
        }
        return paths;
    }

    /** The data files Iceberg plans for the current snapshot. */
    private List<String> icebergFilePaths(Long snapshotId) throws Exception {
        List<String> paths = new ArrayList<>();
        org.apache.iceberg.TableScan scan = getIcebergTable().newScan();
        if (snapshotId != null) {
            scan = scan.useSnapshot(snapshotId);
        }
        try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = scan.planFiles()) {
            for (org.apache.iceberg.FileScanTask task : tasks) {
                paths.add(task.file().path().toString());
            }
        }
        return paths;
    }

    private FileStoreTable createPaimonTable(
            FileIO fileIO,
            String database,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            int numBuckets,
            Map<String, String> customOptions)
            throws Exception {
        Path path = new Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, numBuckets);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.TABLE_LOCATION);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 8);
        options.set(IcebergOptions.METADATA_DELETE_AFTER_COMMIT, true);
        options.set(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX, 1);
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, MemorySize.ofKibiBytes(8));
        Schema schema =
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options.toMap(), "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase(database, false);
            Identifier paimonIdentifier = Identifier.create(database, "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private List<String> getIcebergResult() throws Exception {
        return getIcebergResult(
                icebergTable -> IcebergGenerics.read(icebergTable).build(), Record::toString);
    }

    private org.apache.iceberg.Table getIcebergTable() {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        return icebergCatalog.loadTable(icebergIdentifier);
    }

    private List<String> getIcebergResult(
            Function<org.apache.iceberg.Table, CloseableIterable<Record>> query,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        org.apache.iceberg.Table icebergTable = getIcebergTable();
        CloseableIterable<Record> result = query.apply(icebergTable);
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(icebergRecordToString.apply(record));
        }
        result.close();
        return actual;
    }
}
