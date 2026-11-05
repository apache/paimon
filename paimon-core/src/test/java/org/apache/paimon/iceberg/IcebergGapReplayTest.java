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
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
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
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
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

        // the copies stand alone: with the originals out of the way, every snapshot still reads
        Path metadataDirectory = IcebergCommitCallback.catalogTableMetadataPath(t.enabled);
        for (String name : avroFiles(before)) {
            t.enabled
                    .fileIO()
                    .rename(
                            new Path(metadataDirectory, name),
                            new Path(metadataDirectory, name + ".aside"));
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
        for (String name : avroFiles(before)) {
            t.enabled
                    .fileIO()
                    .rename(
                            new Path(metadataDirectory, name + ".aside"),
                            new Path(metadataDirectory, name));
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
        // Retention drops the base snapshots with their lists and manifests, and the original
        // lists that the first replay left for them, which only their snapshots kept. The
        // copies that the result does not use are gone, and nothing but the result is left.
        assertThat(avroFiles(metadataFileNames(t.enabled)))
                .isEqualTo(filesReachableFrom(t.enabled, m));
        IcebergPathFactory pathFactory = new IcebergPathFactory(metadataDirectory);
        for (String list : headLists) {
            assertThat(t.enabled.fileIO().exists(pathFactory.toManifestListPath(list))).isFalse();
        }
        for (String manifest : headManifests) {
            assertThat(t.enabled.fileIO().exists(pathFactory.toManifestFilePath(manifest)))
                    .isFalse();
        }
        assertThat(difference(avroFiles(before), avroFiles(metadataFileNames(t.enabled))))
                .isEqualTo(avroFiles(before));

        // A replay that keeps the snapshots of its base copies them again, and the originals
        // stay for readers of older versions. Once those version files are deleted, only the
        // snapshots that record them reach them. A rebuild deletes them with the older versions.
        List<Path> originals = replayAndPrune(t, 32, 33);
        t.gap(34, 35);
        t.replay(
                Collections.singletonMap(
                        IcebergOptions.METADATA_MISSING_BASE_STRATEGY.key(), "rebuild"),
                t.identifier,
                new ArrayList<>(),
                NOTHING);
        assertThat(avroFiles(metadataFileNames(t.enabled)))
                .isEqualTo(filesReachableFrom(t.enabled, t.latest()));
        for (Path original : originals) {
            assertThat(t.enabled.fileIO().exists(original)).isFalse();
        }

        // Retention drops a snapshot, and the originals that it records go with it. They stay
        // while the snapshot is kept.
        originals = replayAndPrune(t, 36, 37);
        t.gap(38);
        Map<String, String> keepThree = new HashMap<>(retention);
        t.replay(keepThree, t.identifier, new ArrayList<>(), NOTHING);
        assertThat(readMetadata(t.enabled, t.latest()).snapshots()).hasSize(3);
        for (Path original : originals) {
            assertThat(t.enabled.fileIO().exists(original)).isFalse();
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
        // version 3 lists a manifest that kept its length and lost its contents, which shows
        // only when the copy of its history is decoded
        damageManifest(manifestCreatedAt(t.enabled, 3, 3));
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
                        "REBUILD:10",
                        "BEFORE_WRITE:10",
                        "LOCKED:10",
                        "BEFORE_CLEANUP:10",
                        "CLEANUP_LOCKED:10");
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
                        "REBUILD:" + n,
                        "BEFORE_WRITE:" + n,
                        "LOCKED:" + n,
                        "BEFORE_CLEANUP:" + n,
                        "CLEANUP_LOCKED:" + n);
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
        assertThat(events)
                .containsExactly(
                        "REBUILD:" + n,
                        "BEFORE_WRITE:" + n,
                        "LOCKED:" + n,
                        "BEFORE_CLEANUP:" + n,
                        "CLEANUP_LOCKED:" + n);
        assertRebuilt(t, n, events, before);

        // every limit sends the replay to the rebuild
        for (String budget :
                new String[] {
                    "max-snapshots=1",
                    "max-delta-entries=1",
                    "max-base-manifest-bytes=bytes-of-the-base",
                    "max-duration=0 ms"
                }) {
            String[] parts = budget.split("=");
            t.gap(key++, key++);
            n = t.latest();
            // the bytes of the base manifests pay for copying them, and not for checking them too
            String limit =
                    parts[1].equals("bytes-of-the-base")
                            ? manifestBytes(t.enabled, n - 2) + " b"
                            : parts[1];
            before = metadataFileNames(t.enabled);
            events = new ArrayList<>();
            t.replay(
                    Collections.singletonMap("metadata.iceberg.replay." + parts[0], limit),
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

        // a manifest that is damaged where only its contents show it: one that kept its length
        // and lost what it holds, and one that holds other counts than its list records
        for (boolean miscounted : new boolean[] {false, true}) {
            t.gap(key++, key++);
            n = t.latest();
            long baseVersion = n - 2;
            if (miscounted) {
                miscountFiles(t.enabled, baseVersion);
            } else {
                damageManifest(firstManifestOf(t.enabled, baseVersion));
            }
            before = metadataFileNames(t.enabled);
            events = new ArrayList<>();
            t.replay(t.identifier, events, NOTHING);
            assertThat(events)
                    .containsSubsequence(
                            "BASE_SELECTED:" + baseVersion,
                            "LISTS_READ:" + baseVersion,
                            "CANDIDATE_REJECTED:" + baseVersion,
                            "REBUILD:" + n);
            assertRebuilt(t, n, events, before);
        }

        // a table with a primary key is not replayed, and needs no lock for the rebuild
        ReplayTable keyed =
                new ReplayTable(
                        Collections.singletonList("k"),
                        Collections.emptyMap(),
                        LocalFileIO.create(),
                        "keyed");
        keyed.append(keyed.enabled, 1);
        keyed.append(keyed.enabled, 2);
        keyed.gap(3, 4);
        keyed.lock.absent = true;
        events = new ArrayList<>();
        keyed.replay(keyed.identifier, events, NOTHING);
        assertThat(events)
                .containsExactly(
                        "REBUILD:" + keyed.latest(),
                        "BEFORE_WRITE:" + keyed.latest(),
                        "LOCKED:" + keyed.latest(),
                        "BEFORE_CLEANUP:" + keyed.latest(),
                        "CLEANUP_LOCKED:" + keyed.latest());
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
        long firstIdentifier = t.identifier;
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

        // the version file of an old timeline is in the way, and it is deleted only while this
        // snapshot is the one on disk. The head is replaced after the last check, so the file
        // stays: it belongs to the commit that replaced the snapshot
        t.append(t.enabled, 28);
        t.gap(29, 31);
        n = t.latest();
        long staleId = n;
        long staleIdentifier = t.identifier;
        Path stale = metadataPath(t.enabled, n);
        String staleJson =
                t.enabled
                        .fileIO()
                        .readFileUtf8(metadataPath(t.enabled, readVersionHint(t.enabled)));
        t.enabled.fileIO().writeFile(stale, staleJson, false);
        before = metadataFileNames(t.enabled);
        events = new ArrayList<>();
        interruptOnce(
                t,
                staleIdentifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_WRITE,
                -1,
                () -> {
                    t.enabled.rollbackTo(staleId - 1);
                    t.append(t.disabled, 30);
                });
        assertThat(events).contains("ABANDONED:" + n);
        assertThat(t.enabled.fileIO().readFileUtf8(stale)).isEqualTo(staleJson);
        // the rollback left its marker, and the callback that was replaced does not clear it:
        // it did not list what that rollback left behind
        assertThat(metadataFileNames(t.enabled))
                .isEqualTo(union(before, Collections.singleton("retire-pending")));

        // no version file is in the way, and the head is replaced after the replay finished
        // its work: the metadata of the replaced snapshot is not written, the hint and the
        // catalog do not move, and Iceberg keeps showing what it showed
        t.append(t.enabled, 32);
        t.gap(33, 34);
        n = t.latest();
        long createdId = n;
        long hint = readVersionHint(t.enabled);
        List<String> shown = getIcebergResult();
        before = metadataFileNames(t.enabled);
        events = new ArrayList<>();
        interruptOnce(
                t,
                t.identifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_WRITE,
                -1,
                () -> {
                    t.enabled.rollbackTo(createdId - 1);
                    t.append(t.disabled, 100);
                });
        assertAbandoned(t, n, events, before);
        assertThat(readVersionHint(t.enabled)).isEqualTo(hint);
        assertThat(getIcebergResult()).isEqualTo(shown);

        // A rebuild, an incremental publication and the repair of a retry publish like the replay
        // does. The head is replaced after the metadata is built, and the callback of the
        // replacement publishes first: the old metadata must not be written over it.
        t.append(t.enabled, 38);
        t.gap(39, 40);
        assertReplacedHeadIsNotOverwritten(
                t,
                Collections.singletonMap(
                        IcebergOptions.METADATA_MISSING_BASE_STRATEGY.key(), "rebuild"),
                101);
        t.append(t.enabled, 41);
        t.append(t.disabled, 42);
        assertReplacedHeadIsNotOverwritten(t, Collections.emptyMap(), 102);

        // the repair of a retry moves the hint only while its snapshot is the head
        t.append(t.enabled, 43);
        n = t.latest();
        long repairedId = n;
        Path hintFile = new Path(metadataPath(t.enabled, n).getParent(), "version-hint.text");
        t.enabled.fileIO().overwriteFileUtf8(hintFile, String.valueOf(n - 1));
        events = new ArrayList<>();
        interruptOnce(
                t,
                t.identifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_WRITE,
                -1,
                () -> t.enabled.rollbackTo(repairedId - 1));
        assertThat(events).contains("BEFORE_WRITE:" + n);
        assertThat(readVersionHint(t.enabled)).isEqualTo(n - 1);

        // nor once a newer snapshot has published: the hint is not moved back to the older one
        t.append(t.enabled, 58);
        long olderId = t.latest();
        t.enabled.fileIO().overwriteFileUtf8(hintFile, String.valueOf(olderId - 1));
        events = new ArrayList<>();
        interruptOnce(
                t,
                t.identifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_WRITE,
                -1,
                () -> t.append(t.enabled, 59));
        assertThat(events).contains("BEFORE_WRITE:" + olderId);
        assertThat(t.latest()).isEqualTo(olderId + 1);
        assertThat(readVersionHint(t.enabled)).isEqualTo(olderId + 1);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, olderId + 1));

        // A rollback waits while the metadata of the snapshot is published through the pointer
        // of the catalog, and runs when it is done. It does not matter which handle rolls back:
        // the same table has handles with Iceberg metadata on and off.
        t.append(t.enabled, 35);
        t.gap(36, 37);
        assertWaitsForPublication(t, id -> t.enabled.rollbackTo(id - 1));
        assertThat(t.latest()).isEqualTo(readVersionHint(t.enabled) - 1);
        t.append(t.enabled, 44);
        t.gap(45, 46);
        assertWaitsForPublication(t, id -> t.disabled.rollbackTo(id - 1));
        assertThat(t.latest()).isEqualTo(readVersionHint(t.enabled) - 1);

        // so does the change of the refs in the metadata of the head, which is written in place
        t.append(t.enabled, 47);
        t.gap(48, 49);
        assertWaitsForPublication(t, id -> t.enabled.createTag("waited", id));
        assertThat(t.latest()).isEqualTo(readVersionHint(t.enabled));
        assertThat(readMetadata(t.enabled, t.latest()).refs()).containsKey("waited");

        // a rebuild, an incremental publication and the repair of a retry hold the lock as well
        t.append(t.enabled, 50);
        t.gap(51, 52);
        assertWaitsForPublication(
                t,
                Collections.singletonMap(
                        IcebergOptions.METADATA_MISSING_BASE_STRATEGY.key(), "rebuild"),
                id -> t.enabled.createTag("rebuilt", id));
        t.append(t.enabled, 53);
        t.append(t.disabled, 54);
        assertWaitsForPublication(t, id -> t.enabled.createTag("extended", id));
        t.append(t.enabled, 55);
        t.enabled.fileIO().overwriteFileUtf8(hintFile, String.valueOf(t.latest() - 1));
        assertWaitsForPublication(t, id -> t.enabled.createTag("repaired", id));
        t.append(t.enabled, 56);
        t.gap(57);
        assertWaitsForPublication(t, id -> t.enabled.deleteTag("waited"));
        assertThat(readMetadata(t.enabled, t.latest()).refs()).doesNotContainKey("waited");

        // The files that a publication made unneeded are deleted under the lock too, and only
        // while the publication stands. A rollback and a replacement can come between the
        // publication and the cleanup: the cleanup is abandoned then, because what it would
        // delete belongs to the new timeline. The rollback keeps the snapshot that comes before
        // the ones that the old timeline still has, so the files of the two timelines overlap.
        // A replay, which also drops the originals of the base snapshots:
        t.append(t.enabled, 60);
        t.append(t.enabled, 61);
        t.gap(62, 63);
        assertCleanupLeavesReplacedTimeline(t, deletingVersions(0), t.latest() - 3, 160);
        // a rebuild, which deletes what every older metadata file references:
        t.append(t.enabled, 64);
        t.append(t.enabled, 65);
        t.gap(66, 67);
        assertCleanupLeavesReplacedTimeline(
                t,
                Collections.singletonMap(
                        IcebergOptions.METADATA_MISSING_BASE_STRATEGY.key(), "rebuild"),
                t.latest() - 3,
                161);
        // an incremental publication, which also expires snapshots by retention:
        t.append(t.enabled, 68);
        t.append(t.enabled, 69);
        t.append(t.disabled, 70);
        Map<String, String> expiring = deletingVersions(0);
        expiring.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        expiring.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "2");
        assertCleanupLeavesReplacedTimeline(t, expiring, t.latest() - 2, 162);
        // the repair of a retry:
        t.append(t.enabled, 71);
        t.append(t.enabled, 72);
        t.append(t.enabled, 73);
        assertCleanupLeavesReplacedTimeline(t, deletingVersions(0), t.latest() - 2, 163);
        // a cleanup that runs in several turns checks again in every one of them
        t.append(t.enabled, 74);
        t.append(t.enabled, 75);
        t.gap(76, 77);
        t.cleanupBatchSize = 1;
        assertCleanupLeavesReplacedTimeline(t, deletingVersions(0), t.latest() - 3, 164, 1);
        t.cleanupBatchSize = 0;

        // The versions above the head are retired under the lock, and a version that a commit
        // published after they were listed is kept
        t.append(t.enabled, 78);
        t.append(t.enabled, 79);
        t.append(t.enabled, 80);
        long retained = t.latest() - 1;
        long retainedIdentifier = t.identifier - 1;
        t.enabled.rollbackTo(retained);
        assertThat(versionFiles(t.enabled)).contains("v" + (retained + 1) + ".metadata.json");
        events = new ArrayList<>();
        interruptOnce(
                t,
                Collections.emptyMap(),
                retainedIdentifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_CLEANUP,
                -1,
                () -> t.append(t.enabled, 81));
        assertThat(events).contains("CLEANUP_LOCKED:" + retained);
        assertThat(t.latest()).isEqualTo(retained + 1);
        assertThat(readVersionHint(t.enabled)).isEqualTo(retained + 1);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, retained + 1));

        // and so is the marker that a rollback left, unless another rollback wrote it again
        // after the versions were listed: that one left versions that are not in the listing
        t.append(t.enabled, 82);
        t.append(t.enabled, 83);
        retained = t.latest() - 1;
        retainedIdentifier = t.identifier - 1;
        t.enabled.rollbackTo(retained);
        long again = retained;
        events = new ArrayList<>();
        interruptOnce(
                t,
                Collections.emptyMap(),
                retainedIdentifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_CLEANUP,
                -1,
                () -> {
                    t.append(t.enabled, 84);
                    t.enabled.rollbackTo(again);
                });
        assertThat(versionFiles(t.enabled)).doesNotContain("v" + (retained + 1) + ".metadata.json");
        assertThat(metadataFileNames(t.enabled)).contains("retire-pending");

        // the same holds when the callback publishes the metadata of the snapshot itself
        t.append(t.enabled, 85);
        t.append(t.enabled, 86);
        t.append(t.enabled, 87);
        long published = t.latest() - 1;
        t.enabled.rollbackTo(published - 1);
        t.append(t.disabled, 88);
        events = new ArrayList<>();
        interruptOnce(
                t,
                Collections.emptyMap(),
                t.identifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_CLEANUP,
                -1,
                () -> {
                    t.append(t.enabled, 89);
                    t.enabled.rollbackTo(published);
                });
        assertThat(events).contains("CLEANUP_LOCKED:" + published);
        assertThat(versionFiles(t.enabled))
                .doesNotContain("v" + (published + 1) + ".metadata.json");
        assertThat(metadataFileNames(t.enabled)).contains("retire-pending");

        // the first snapshot wipes the metadata directory of an older table, and only while it
        // is the head: a callback that comes late does not wipe what later commits published
        Set<String> versions = versionFiles(t.enabled);
        events = new ArrayList<>();
        t.replay(firstIdentifier, events, NOTHING);
        assertThat(events).contains("BEFORE_CLEANUP:1").doesNotContain("CLEANUP_LOCKED:1");
        assertThat(versionFiles(t.enabled)).isEqualTo(versions);

        // Every one of these deletions holds the lock while it runs
        Map<String, String> rebuild =
                Collections.singletonMap(
                        IcebergOptions.METADATA_MISSING_BASE_STRATEGY.key(), "rebuild");
        t.append(t.enabled, 90);
        t.gap(91, 92);
        assertCleanupWaits(t, rebuild, t.identifier, "rebuild", 0);
        t.append(t.enabled, 93);
        t.append(t.disabled, 94);
        assertCleanupWaits(t, deletingVersions(1), t.identifier, "incremental", 0);
        t.append(t.enabled, 95);
        t.gap(96, 97);
        assertCleanupWaits(t, deletingVersions(1), t.identifier, "replay", 0);
        t.append(t.enabled, 98);
        assertCleanupWaits(t, deletingVersions(0), t.identifier, "repair", 0);
        // the versions above the head after a rollback, and then the marker of the rollback
        t.append(t.enabled, 99);
        t.append(t.enabled, 100);
        t.append(t.enabled, 101);
        t.enabled.rollbackTo(t.latest() - 1);
        assertCleanupWaits(t, Collections.emptyMap(), t.identifier - 1, "retire", 0);
        t.append(t.enabled, 102);
        t.append(t.enabled, 103);
        t.enabled.rollbackTo(t.latest() - 1);
        assertCleanupWaits(t, Collections.emptyMap(), t.identifier - 1, "marker", 1);
        // the wipe of the metadata directory by the first snapshot
        ReplayTable fresh =
                new ReplayTable(
                        Collections.emptyList(), Collections.emptyMap(), LocalFileIO.create(), "w");
        fresh.append(fresh.disabled, 1);
        assertCleanupWaits(fresh, Collections.emptyMap(), fresh.identifier, "wipe", 0);

        // Two callbacks can build metadata from the same snapshot, with different files, and only
        // one of them is durable. The one that lost owns none of what the winner lists
        assertLosingRebuildKeepsWhatTheWinnerLists();
        assertLosingIncrementalBuildKeepsWhatTheWinnerLists();
        // and a cleanup is for the metadata it was chosen for, not for any metadata of the commit
        assertCleanupsLeaveAnotherGeneration();
    }

    /**
     * A rebuild is paused right before it publishes snapshot 5, while the metadata of snapshot 4 is
     * missing. Other callbacks publish snapshot 4, and then snapshot 5 on top of it. The write of
     * the rebuild loses to that file, which lists what the older versions list. A rebuild deletes
     * these files when it wins, so this one deletes nothing, and the catalog gets the metadata that
     * stayed and not the one the rebuild built.
     */
    private void assertLosingRebuildKeepsWhatTheWinnerLists() throws Exception {
        // a catalog that is given the metadata itself, and not its path, like a REST catalog.
        // The metadata is stored with the table, where Iceberg reads it
        Map<String, String> options = new HashMap<>();
        options.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                IcebergOptions.StorageType.HADOOP_CATALOG.toString());
        options.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE_LOCATION.key(),
                IcebergOptions.StorageLocation.TABLE_LOCATION.toString());
        RecordingIcebergMetadataCommitter.commitsMetadata = true;
        try {
            ReplayTable t =
                    new ReplayTable(
                            Collections.emptyList(),
                            options,
                            LocalFileIO.create(),
                            "loser_rebuild");
            t.append(t.enabled, 1);
            t.append(t.enabled, 2);
            t.append(t.enabled, 3);
            t.gap(4, 5);
            long n = t.latest();
            long identifier = t.identifier;
            List<String> events = new ArrayList<>();
            String[] winner = new String[1];
            RecordingIcebergMetadataCommitter.COMMITS.clear();
            interruptOnce(
                    t,
                    Collections.singletonMap(
                            IcebergOptions.METADATA_MISSING_BASE_STRATEGY.key(), "rebuild"),
                    identifier,
                    events,
                    IcebergCommitCallback.ReplayPoint.BEFORE_WRITE,
                    -1,
                    () -> {
                        t.replay(identifier - 1, new ArrayList<>(), NOTHING);
                        t.replay(identifier, new ArrayList<>(), NOTHING);
                        winner[0] = t.enabled.fileIO().readFileUtf8(metadataPath(t.enabled, n));
                    });
            assertThat(events).contains("LOCKED:" + n);
            assertThat(events).noneMatch(event -> event.startsWith("BEFORE_CLEANUP"));
            assertThat(t.enabled.fileIO().readFileUtf8(metadataPath(t.enabled, n)))
                    .isEqualTo(winner[0]);
            // the winner is the incremental one, which keeps every snapshot
            assertThat(readMetadata(t.enabled, n).snapshots()).hasSize((int) n);
            assertIcebergReadsEverySnapshot(t);

            // the catalog was given the winner's metadata, and then the metadata that stayed
            // again when the rebuild lost, and never the metadata that the rebuild built
            String durable = readMetadata(t.enabled, n).currentSnapshot().manifestList();
            assertThat(RecordingIcebergMetadataCommitter.COMMITS).hasSize(2);
            for (Object commit : RecordingIcebergMetadataCommitter.COMMITS) {
                assertThat(((IcebergMetadata) commit).currentSnapshot().manifestList())
                        .isEqualTo(durable);
            }
        } finally {
            RecordingIcebergMetadataCommitter.commitsMetadata = false;
            RecordingIcebergMetadataCommitter.COMMITS.clear();
            RecordingIcebergMetadataCommitter.BASES.clear();
        }
    }

    /**
     * An incremental build that retention lets drop the older snapshots is paused right before it
     * publishes. Another callback, with the retention of the table, publishes the same snapshot and
     * keeps them. The write of the first loses, and it does not delete what it would have dropped.
     */
    private void assertLosingIncrementalBuildKeepsWhatTheWinnerLists() throws Exception {
        ReplayTable t =
                new ReplayTable(
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        LocalFileIO.create(),
                        "loser_incremental");
        for (int key = 1; key <= 4; key++) {
            t.append(t.enabled, key);
        }
        t.append(t.disabled, 5);
        long n = t.latest();
        long identifier = t.identifier;
        Map<String, String> tight = new HashMap<>();
        tight.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "2");
        tight.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "2");
        List<String> events = new ArrayList<>();
        interruptOnce(
                t,
                tight,
                identifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_WRITE,
                -1,
                () -> t.replay(identifier, new ArrayList<>(), NOTHING));
        assertThat(events).contains("LOCKED:" + n);
        assertThat(events).noneMatch(event -> event.startsWith("BEFORE_CLEANUP"));
        assertThat(readMetadata(t.enabled, n).snapshots()).hasSize((int) n);
        assertIcebergReadsEverySnapshot(t);
    }

    /**
     * A cleanup is paused right before it deletes. The metadata file of the head is replaced by
     * another generation of the same commit: another list, the same files. That is what a callback
     * that wrote the file again leaves after the first one was lost. Nothing is deleted, whichever
     * callback the cleanup is of.
     */
    private void assertCleanupsLeaveAnotherGeneration() throws Exception {
        ReplayTable t =
                new ReplayTable(
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        LocalFileIO.create(),
                        "generation");
        t.append(t.enabled, 1);
        t.append(t.enabled, 2);
        t.gap(3, 4);
        assertCleanupLeavesAnotherGeneration(
                t,
                Collections.singletonMap(
                        IcebergOptions.METADATA_MISSING_BASE_STRATEGY.key(), "rebuild"),
                t.identifier,
                "rebuild");
        t.append(t.enabled, 5);
        t.append(t.disabled, 6);
        assertCleanupLeavesAnotherGeneration(t, deletingVersions(1), t.identifier, "incremental");
        t.append(t.enabled, 7);
        t.gap(8, 9);
        assertCleanupLeavesAnotherGeneration(t, deletingVersions(1), t.identifier, "replay");
        t.append(t.enabled, 10);
        assertCleanupLeavesAnotherGeneration(t, deletingVersions(0), t.identifier, "repair");
    }

    private void assertCleanupLeavesAnotherGeneration(
            ReplayTable t, Map<String, String> options, long identifier, String cleanup)
            throws Exception {
        long n = t.latest();
        List<String> events = new ArrayList<>();
        Set<String> before = new HashSet<>();
        interruptOnce(
                t,
                options,
                identifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_CLEANUP,
                -1,
                () -> {
                    rewriteCurrentList(t.enabled, n, metas -> {});
                    before.addAll(metadataFileNames(t.enabled));
                });
        assertThat(events).as(cleanup).contains("BEFORE_CLEANUP:" + n);
        assertThat(events).as(cleanup).noneMatch(event -> event.startsWith("CLEANUP_LOCKED"));
        assertThat(metadataFileNames(t.enabled)).as(cleanup).containsAll(before);
    }

    /** Iceberg plans the files that Paimon has for each of the snapshots the head lists. */
    private void assertIcebergReadsEverySnapshot(ReplayTable t) throws Exception {
        org.apache.iceberg.Table table = getIcebergTable(t.database);
        for (org.apache.iceberg.Snapshot snapshot : table.snapshots()) {
            List<String> planned = new ArrayList<>();
            try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
                    table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
                for (org.apache.iceberg.FileScanTask task : tasks) {
                    planned.add(task.file().path().toString());
                }
            }
            assertThat(planned)
                    .as("snapshot " + snapshot.snapshotId())
                    .containsExactlyInAnyOrderElementsOf(
                            paimonFilePaths(t.enabled, snapshot.snapshotId()));
        }
    }

    private static Map<String, String> deletingVersions(int kept) {
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.METADATA_DELETE_AFTER_COMMIT.key(), "true");
        options.put(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX.key(), String.valueOf(kept));
        return options;
    }

    /**
     * A callback is paused after it published the metadata of the head, right before it deletes
     * what the publication made unneeded. The table is rolled back to a snapshot that the files of
     * the old timeline were built from, and another commit publishes the next snapshot. Then the
     * paused callback goes on: it deletes nothing, and Iceberg shows the new timeline.
     */
    private void assertCleanupLeavesReplacedTimeline(
            ReplayTable t, Map<String, String> options, long keep, int replacementKey)
            throws Exception {
        assertCleanupLeavesReplacedTimeline(t, options, keep, replacementKey, 0);
    }

    private void assertCleanupLeavesReplacedTimeline(
            ReplayTable t,
            Map<String, String> options,
            long keep,
            int replacementKey,
            int turnsBefore)
            throws Exception {
        long n = t.latest();
        List<String> events = new ArrayList<>();
        interruptAt(
                t,
                options,
                t.identifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_CLEANUP,
                -1,
                turnsBefore,
                () -> {
                    t.enabled.rollbackTo(keep);
                    t.append(t.enabled, replacementKey);
                });
        assertThat(events).contains("BEFORE_CLEANUP:" + n);
        assertThat(events)
                .filteredOn(event -> event.startsWith("CLEANUP_LOCKED"))
                .hasSize(turnsBefore);
        long head = t.latest();
        assertThat(head).isEqualTo(keep + 1);
        assertThat(readVersionHint(t.enabled)).isEqualTo(head);
        assertThat(versionFiles(t.enabled)).contains("v" + head + ".metadata.json");
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, head));
    }

    /**
     * An operation on the table waits while a cleanup holds the lock, and runs when the cleanup is
     * done. The callback is the one for the head, and the lock is taken for the (turn + 1)th time
     * at the point.
     */
    private void assertCleanupWaits(
            ReplayTable t, Map<String, String> options, long identifier, String cleanup, int turn)
            throws Exception {
        assertWaitsAt(
                t,
                options,
                identifier,
                IcebergCommitCallback.ReplayPoint.CLEANUP_LOCKED,
                turn,
                id -> t.enabled.createTag(cleanup, id));
        assertThat(t.enabled.tagManager().tagExists(cleanup)).as(cleanup).isTrue();
    }

    /**
     * A callback is paused right before it publishes the metadata of the head. A rollback and
     * another commit replace the head, and the callback of that commit publishes. Then the paused
     * callback goes on, and what the replacement published is left as it is.
     */
    private void assertReplacedHeadIsNotOverwritten(
            ReplayTable t, Map<String, String> options, int replacementKey) throws Exception {
        long n = t.latest();
        List<String> events = new ArrayList<>();
        interruptOnce(
                t,
                options,
                t.identifier,
                events,
                IcebergCommitCallback.ReplayPoint.BEFORE_WRITE,
                -1,
                () -> {
                    t.enabled.rollbackTo(n - 1);
                    t.append(t.disabled, replacementKey);
                    t.replay(t.identifier, new ArrayList<>(), NOTHING);
                });
        assertThat(events).contains("BEFORE_WRITE:" + n);
        assertThat(events).noneMatch(event -> event.startsWith("EXPOSED"));
        assertThat(readVersionHint(t.enabled)).isEqualTo(n);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrderElementsOf(paimonRows(t.enabled, n));
    }

    /**
     * An operation on the table waits for a publication that is under way, and runs when it is
     * done. It is given the id of the snapshot that is published.
     */
    private void assertWaitsForPublication(ReplayTable t, LongConsumer operation) throws Exception {
        assertWaitsForPublication(t, Collections.emptyMap(), operation);
    }

    private void assertWaitsForPublication(
            ReplayTable t, Map<String, String> options, LongConsumer operation) throws Exception {
        long n = t.latest();
        assertWaitsAt(
                t, options, t.identifier, IcebergCommitCallback.ReplayPoint.LOCKED, 0, operation);
        assertThat(readVersionHint(t.enabled)).isEqualTo(n);
    }

    /**
     * An operation on the table waits while a callback holds the lock at a point of its, and runs
     * when the callback is done. The point is the one that the callback reaches for the (skipped +
     * 1)th time. The operation is given the head at the start.
     */
    private void assertWaitsAt(
            ReplayTable t,
            Map<String, String> options,
            long identifier,
            IcebergCommitCallback.ReplayPoint point,
            int skipped,
            LongConsumer operation)
            throws Exception {
        long n = t.latest();
        List<String> events = new ArrayList<>();
        ExecutorService operations = Executors.newSingleThreadExecutor();
        try {
            AtomicReference<Future<?>> waiting = new AtomicReference<>();
            interruptAt(
                    t,
                    options,
                    identifier,
                    events,
                    point,
                    -1,
                    skipped,
                    () -> {
                        waiting.set(operations.submit(() -> operation.accept(n)));
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                        while (!t.lock.lock.hasQueuedThreads()) {
                            assertThat(System.nanoTime())
                                    .as("the operation waits")
                                    .isLessThan(deadline);
                            Thread.sleep(10);
                        }
                        assertThat(waiting.get().isDone()).isFalse();
                        assertThat(t.latest()).isEqualTo(n);
                    });
            assertThat(waiting.get()).as("the callback reached " + point).isNotNull();
            waiting.get().get(30, TimeUnit.SECONDS);
        } finally {
            operations.shutdownNow();
        }
        assertThat(events).contains(point + ":" + n);
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
        interruptAt(t, options, commitIdentifier, events, at, atValue, 0, interruption);
    }

    /** The same, at the (skipped + 1)th time that the replay reaches the point. */
    private void interruptAt(
            ReplayTable t,
            Map<String, String> options,
            long commitIdentifier,
            List<String> events,
            IcebergCommitCallback.ReplayPoint at,
            long atValue,
            int skipped,
            ReplayInterruption interruption)
            throws Exception {
        AtomicInteger reached = new AtomicInteger();
        t.replay(
                options,
                commitIdentifier,
                events,
                (point, value) -> {
                    if (point == at
                            && (atValue < 0 || value == atValue)
                            && reached.getAndIncrement() == skipped) {
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
        COPY_UNREADABLE,
        COPY_CUT_SHORT,
        COPY_CUT_AT_BLOCK,
        LOCK_UNAVAILABLE,
        NO_LOCK,
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
                case COPY_UNREADABLE:
                    // the copies are new files, and the read that checks one of them fails
                    fileIO.failReadsOf(
                            name -> name.matches(".*-m\\d+\\.avro") && !before.contains(name),
                            new IOException("injected storage failure"));
                    break;
                case COPY_CUT_SHORT:
                    // a read that ends early may be a file that is damaged, or a stream that
                    // closed early, and the replay cannot tell which
                    fileIO.cutReadsOf(
                            name -> name.matches(".*-m\\d+\\.avro") && !before.contains(name),
                            IcebergGapReplayTest::halfOf);
                    break;
                case COPY_CUT_AT_BLOCK:
                    // the same, where a block ends, which a reader can take for the end of file
                    fileIO.cutReadsOf(
                            name -> name.matches(".*-m\\d+\\.avro") && !before.contains(name),
                            IcebergGapReplayTest::avroHeaderEnd);
                    break;
                case LOCK_UNAVAILABLE:
                    t.lock.unavailable = true;
                    break;
                case NO_LOCK:
                    // the catalog has no lock, so nothing keeps a rollback out of the publication
                    t.lock.absent = true;
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
                    .hasStackTraceContaining(
                            failure == Failure.COPY_CUT_SHORT
                                            || failure == Failure.COPY_CUT_AT_BLOCK
                                    ? "ended before the end of the file"
                                    : failure == Failure.NO_LOCK ? "needs a lock" : "injected");
            fileIO.stopFailing();
            t.lock.unavailable = false;
            t.lock.absent = false;
            RecordingIcebergMetadataCommitter.failNextCommit = false;

            // a failure is no reason to read the whole table
            assertThat(events).noneMatch(event -> event.startsWith("REBUILD"));
            assertThat(RecordingIcebergMetadataCommitter.COMMITS).isEmpty();
            Set<String> failed = metadataFileNames(t.enabled);
            switch (failure) {
                case BEFORE_WRITE:
                case COPY_UNREADABLE:
                case COPY_CUT_SHORT:
                case COPY_CUT_AT_BLOCK:
                case LOCK_UNAVAILABLE:
                case NO_LOCK:
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
     * A file system that fails, or cuts short, every read of the files that match a name, to look
     * like a storage failure, and that can make copies slow.
     */
    private static class FaultyFileIO extends LocalFileIO {

        private volatile Predicate<String> failing = name -> false;
        private volatile IOException failure;
        private volatile ToLongFunction<Path> cutAt;
        private volatile long copyDelayMillis;
        private final AtomicInteger copies = new AtomicInteger();

        private void failReadsOf(String name, IOException failure) {
            failReadsOf(name::equals, failure);
        }

        private void failReadsOf(Predicate<String> names, IOException failure) {
            this.failing = names;
            this.failure = failure;
            this.cutAt = null;
        }

        /** Reads of the files end at a position before their end, with no error. */
        private void cutReadsOf(Predicate<String> names, ToLongFunction<Path> position) {
            this.failing = names;
            this.failure = null;
            this.cutAt = position;
        }

        private void stopFailing() {
            failReadsOf(name -> false, null);
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            SeekableInputStream in = super.newInputStream(path);
            if (!failing.test(path.getName())) {
                return in;
            }
            ToLongFunction<Path> position = cutAt;
            if (position == null) {
                in.close();
                throw failure;
            }
            return new CutShortStream(in, position.applyAsLong(path));
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

    /**
     * A table lock that a test can watch, make unavailable, and take away. It is not reentrant, so
     * an operation that takes it while it holds it waits, and the wait fails the test.
     */
    private static class ReplayLock implements CatalogLockFactory {

        private final Semaphore lock = new Semaphore(1);
        private volatile boolean unavailable;
        private volatile boolean absent;

        @Override
        public String identifier() {
            return "replay-test-lock";
        }

        @Override
        public Lock createLock(
                CatalogLockContext context,
                Identifier identifier,
                String tableUuid,
                String commitUser,
                Options tableOptions) {
            if (absent) {
                // a catalog that has no lock
                return Lock.empty();
            }
            return new Lock() {
                @Override
                public AutoCloseable acquire() throws Exception {
                    if (unavailable) {
                        throw new IOException("injected lock failure");
                    }
                    if (!lock.tryAcquire(30, TimeUnit.SECONDS)) {
                        throw new IOException("the lock was not released in 30 seconds");
                    }
                    return lock::release;
                }

                @Override
                public void close() {}
            };
        }
    }

    /** A stream that reports the end of the file at a position before its real end. */
    private static class CutShortStream extends SeekableInputStream {

        private final SeekableInputStream in;
        private final long limit;

        private CutShortStream(SeekableInputStream in, long limit) {
            this.in = in;
            this.limit = limit;
        }

        @Override
        public void seek(long desired) throws IOException {
            in.seek(desired);
        }

        @Override
        public long getPos() throws IOException {
            return in.getPos();
        }

        @Override
        public int read() throws IOException {
            return in.getPos() >= limit ? -1 : in.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            long left = limit - in.getPos();
            return left <= 0 ? -1 : in.read(b, off, (int) Math.min(len, left));
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /** A table with Iceberg metadata on, and the same table with it off, to leave gaps. */
    private final class ReplayTable {

        private final String database;
        private final FileStoreTable enabled;
        private final FileStoreTable disabled;
        private final String user = UUID.randomUUID().toString();
        private final ReplayLock lock = new ReplayLock();
        private long identifier = 0;
        /** How many files a cleanup deletes for each hold of the lock, the default when 0. */
        private int cleanupBatchSize = 0;

        private ReplayTable(Map<String, String> options) throws Exception {
            this(Collections.emptyList(), options, LocalFileIO.create(), "mydb");
        }

        private ReplayTable(
                List<String> primaryKeys,
                Map<String, String> options,
                FileIO fileIO,
                String database)
                throws Exception {
            this.database = database;
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
            // the table takes the lock of a catalog, as a table of a catalog that has one does
            CatalogEnvironment plain = table.catalogEnvironment();
            table =
                    FileStoreTableFactory.create(
                            fileIO,
                            table.location(),
                            table.schema(),
                            new CatalogEnvironment(
                                    plain.identifier(),
                                    plain.uuid(),
                                    null,
                                    lock,
                                    null,
                                    null,
                                    false,
                                    false));
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
            if (cleanupBatchSize > 0) {
                callback.setCleanupBatchSize(cleanupBatchSize);
            }
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

    /** The bytes of the manifests that the snapshots of a metadata version list. */
    private long manifestBytes(FileStoreTable table, long version) throws IOException {
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        long bytes = 0;
        for (String name : manifestNames(table, version)) {
            bytes += table.fileIO().getFileSize(pathFactory.toManifestFilePath(name));
        }
        return bytes;
    }

    /**
     * Leaves a gap and replays it, with the version files that are older than the result deleted
     * afterwards. Returns the original lists that the retained snapshots of the result record,
     * which no version file reaches any more.
     */
    private List<Path> replayAndPrune(ReplayTable t, int... keys) throws Exception {
        t.gap(keys);
        t.replay(
                Collections.singletonMap(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX.key(), "0"),
                t.identifier,
                new ArrayList<>(),
                NOTHING);
        long n = t.latest();
        assertThat(versionFiles(t.enabled)).containsExactly("v" + n + ".metadata.json");
        List<Path> originals = new ArrayList<>();
        for (IcebergSnapshot snapshot : readMetadata(t.enabled, n).snapshots()) {
            String json =
                    snapshot.summary().get(IcebergCommitCallback.SNAPSHOT_SUMMARY_REPLAY_ORIGINALS);
            if (json != null) {
                for (String original : JsonSerdeUtil.fromJson(json, String[].class)) {
                    originals.add(
                            new IcebergPathFactory(
                                            IcebergCommitCallback.catalogTableMetadataPath(
                                                    t.enabled))
                                    .toManifestListPath(original));
                }
            }
        }
        assertThat(originals).isNotEmpty();
        for (Path original : originals) {
            assertThat(t.enabled.fileIO().exists(original)).isTrue();
            assertThat(filesReachableFrom(t.enabled, n)).doesNotContain(original.getName());
        }
        return originals;
    }

    /** The first manifest of the oldest snapshot that a metadata version keeps. */
    private Path firstManifestOf(FileStoreTable table, long version) {
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        IcebergManifestList manifestList = IcebergManifestList.create(table, pathFactory);
        IcebergSnapshot oldest = readMetadata(table, version).snapshots().get(0);
        return new Path(
                manifestList.read(new Path(oldest.manifestList()).getName()).get(0).manifestPath());
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
        rewriteCurrentList(table, version, metas -> metas.remove(metas.size() - 1));
    }

    /** Makes the list of the current snapshot record one more added file than a manifest has. */
    private void miscountFiles(FileStoreTable table, long version) throws Exception {
        rewriteCurrentList(
                table,
                version,
                metas -> {
                    IcebergManifestFileMeta meta = metas.get(0);
                    metas.set(
                            0,
                            new IcebergManifestFileMeta(
                                    meta.manifestPath(),
                                    meta.manifestLength(),
                                    meta.partitionSpecId(),
                                    meta.content(),
                                    meta.sequenceNumber(),
                                    meta.minSequenceNumber(),
                                    meta.addedSnapshotId(),
                                    meta.addedFilesCount() + 1,
                                    meta.existingFilesCount(),
                                    meta.deletedFilesCount(),
                                    meta.addedRowsCount(),
                                    meta.existingRowsCount(),
                                    meta.deletedRowsCount(),
                                    meta.partitions(),
                                    meta.firstRowId()));
                });
    }

    private void rewriteCurrentList(
            FileStoreTable table, long version, Consumer<List<IcebergManifestFileMeta>> change)
            throws Exception {
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table));
        IcebergManifestList manifestList = IcebergManifestList.create(table, pathFactory);
        IcebergMetadata metadata = readMetadata(table, version);
        IcebergSnapshot current = metadata.currentSnapshot();
        List<IcebergManifestFileMeta> metas =
                new ArrayList<>(manifestList.read(new Path(current.manifestList()).getName()));
        change.accept(metas);
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

    /**
     * Zeroes what a manifest holds after its header, and keeps its length, so only decoding the
     * file shows it.
     */
    private void damageManifest(Path manifest) throws IOException {
        java.nio.file.Path file = java.nio.file.Paths.get(manifest.toUri().getPath());
        byte[] bytes = java.nio.file.Files.readAllBytes(file);
        int headerEnd = avroHeaderEnd(bytes);
        assertThat(headerEnd).isLessThan(bytes.length - 16 - 32);
        Arrays.fill(bytes, headerEnd, bytes.length - 16, (byte) 0);
        java.nio.file.Files.write(file, bytes);
    }

    /** Where the first block of an Avro file starts. Every block ends with the sync marker. */
    private static int avroHeaderEnd(byte[] bytes) {
        byte[] sync = Arrays.copyOfRange(bytes, bytes.length - 16, bytes.length);
        for (int i = 0; ; i++) {
            if (Arrays.equals(Arrays.copyOfRange(bytes, i, i + 16), sync)) {
                return i + 16;
            }
        }
    }

    private static long avroHeaderEnd(Path file) {
        try {
            return avroHeaderEnd(
                    java.nio.file.Files.readAllBytes(
                            java.nio.file.Paths.get(file.toUri().getPath())));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long halfOf(Path file) {
        return new java.io.File(file.toUri().getPath()).length() / 2;
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
        return getIcebergTable("mydb");
    }

    private org.apache.iceberg.Table getIcebergTable(String database) {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of(database + ".db", "t");
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
