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
import org.apache.paimon.Snapshot;
import org.apache.paimon.blob.ManagedBlobReachabilityCollector;
import org.apache.paimon.blob.ManagedBlobReachabilityCollector.Result;
import org.apache.paimon.blob.ManagedBlobReferenceFile;
import org.apache.paimon.blob.ManagedBlobReferenceFile.Reference;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DataFilePathFactories;

import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests orphan-file cleanup of unreferenced primary-key managed BLOB packs. */
public class ManagedBlobOrphanFilesCleanTest extends TableTestBase {

    @Test
    public void testDeleteUnreferencedPack() throws Exception {
        FileStoreTable table = createManagedBlobTable("orphan_pack");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1, 2})));

        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        List<Path> deleted = clean(table);
        assertThat(table.fileIO().exists(orphan)).isFalse();
        assertThat(deleted).extracting(Path::getName).contains("orphan.managed.blob");
        assertThat(managedBlobs(table)).isNotEmpty();
    }

    @Test
    public void testKeepReferencedPack() throws Exception {
        FileStoreTable table = createManagedBlobTable("keep_pack");
        write(
                table,
                GenericRow.of(
                        1, BinaryString.fromString("a"), new BlobData(new byte[] {9, 8, 7})));
        List<Path> before = managedBlobs(table);
        assertThat(before).isNotEmpty();

        List<Path> deleted = clean(table);

        assertThat(deleted).doesNotContainAnyElementsOf(before);
        for (Path pack : before) {
            assertThat(table.fileIO().exists(pack)).isTrue();
        }
        assertThat(read(table)).hasSize(1);
        assertThat(read(table).get(0).getBlob(2).toData()).containsExactly((byte) 9, (byte) 8, (byte) 7);
    }

    @Test
    public void testEmptySidecarDoesNotBlockOthers() throws Exception {
        FileStoreTable table = createManagedBlobTable("empty_sidecar");
        write(table, GenericRow.of(1, BinaryString.fromString("a"), null));

        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        List<Path> deleted = clean(table);
        assertThat(table.fileIO().exists(orphan)).isFalse();
        assertThat(deleted).extracting(Path::getName).contains("orphan.managed.blob");
    }

    @Test
    public void testMissingSidecarSkipsAllPacks() throws Exception {
        FileStoreTable table = createManagedBlobTable("missing_sidecar");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1})));
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        deleteSidecars(table);
        List<Path> referenced = managedBlobs(table);
        referenced.remove(orphan);

        clean(table);

        assertThat(table.fileIO().exists(orphan)).isTrue();
        for (Path pack : referenced) {
            assertThat(table.fileIO().exists(pack)).isTrue();
        }
    }

    @Test
    public void testCorruptSidecarSkipsAllPacks() throws Exception {
        FileStoreTable table = createManagedBlobTable("corrupt_sidecar");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1})));
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        overwriteSidecars(
                table,
                out -> {
                    out.writeInt(0x50424C52);
                    out.writeByte(1);
                    out.writeInt(0);
                    out.writeInt(12345);
                });

        clean(table);
        assertThat(table.fileIO().exists(orphan)).isTrue();
    }

    @Test
    public void testUnsupportedVersionSkipsAllPacks() throws Exception {
        FileStoreTable table = createManagedBlobTable("unsupported_sidecar");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1})));
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        overwriteSidecars(
                table,
                out -> {
                    out.writeInt(0x50424C52);
                    out.writeByte(99);
                    out.writeInt(0);
                });

        clean(table);
        assertThat(table.fileIO().exists(orphan)).isTrue();
    }

    @Test
    public void testUnreferencedAfterUpdateAndExpire() throws Exception {
        FileStoreTable table = createManagedBlobTable("update_expire");
        write(
                table,
                GenericRow.of(
                        1, BinaryString.fromString("old"), new BlobData(new byte[] {1, 1})));
        write(
                table,
                GenericRow.of(
                        1, BinaryString.fromString("new"), new BlobData(new byte[] {2, 2})));
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        Map<String, String> expire = new HashMap<>();
        expire.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        expire.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        expire.put(CoreOptions.SNAPSHOT_EXPIRE_LIMIT.key(), "10");
        try (org.apache.paimon.table.sink.TableCommitImpl commit =
                table.copy(expire).newCommit("")) {
            commit.expireSnapshots();
        }

        Set<String> live = livePackNames(table);
        assertThat(live).isNotEmpty();
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        List<Path> deleted = clean(table);

        assertThat(table.fileIO().exists(orphan)).isFalse();
        assertThat(deleted).extracting(Path::getName).contains("orphan.managed.blob");
        for (Path pack : managedBlobs(table)) {
            assertThat(live).contains(pack.getName());
        }
        assertThat(read(table)).hasSize(1);
        assertThat(read(table).get(0).getBlob(2).toData()).containsExactly((byte) 2, (byte) 2);
    }

    /**
     * Compaction can commit after orphan GC has listed snapshots. Expire then deletes compact-before
     * data files and blobrefs while those snapshots' manifests are still readable. A used-file scan
     * of the stale list therefore neither skips nor retains the reused pack. Commit/expiration do
     * not forbid this interleaving; production GC is best-effort.
     */
    @Test
    public void testStaleSnapshotListMissesReusedPackAfterCompactBeforeDeleted()
            throws Exception {
        FileStoreTable table = createManagedBlobTable("stale_list_compact");
        write(
                table,
                GenericRow.of(
                        1, BinaryString.fromString("old"), new BlobData(new byte[] {3, 3})));
        write(
                table,
                GenericRow.of(
                        1, BinaryString.fromString("new"), new BlobData(new byte[] {4, 4})));
        List<Snapshot> listed =
                new ArrayList<>(table.snapshotManager().safelyGetAllSnapshots());
        assertThat(listed).isNotEmpty();

        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        Set<String> liveAfterCompact = livePackNames(table);
        assertThat(liveAfterCompact).isNotEmpty();

        List<Path> compactBefore =
                table.store()
                        .newSnapshotDeletion()
                        .planDeletedInDeltaManifest(
                                table.snapshotManager().latestSnapshot(), entry -> false);
        assertThat(compactBefore).isNotEmpty();
        for (Path path : compactBefore) {
            table.fileIO().deleteQuietly(path);
        }

        StaleScan stale = collectUsedPacks(table, listed);
        assertThat(stale.skip).isFalse();
        assertThat(stale.packs).doesNotContainAnyElementsOf(liveAfterCompact);
        assertThat(read(table)).hasSize(1);
        assertThat(read(table).get(0).getBlob(2).toData()).containsExactly((byte) 4, (byte) 4);
    }

    private FileStoreTable createManagedBlobTable(String name) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("payload", DataTypes.BLOB())
                        .primaryKey("id")
                        .option(CoreOptions.BLOB_FIELD.key(), "payload")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "none")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .build();
        catalog.createTable(identifier(name), schema, true);
        return getTable(identifier(name));
    }

    private static List<Path> clean(FileStoreTable table) throws Exception {
        return new LocalOrphanFilesClean(
                        table, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2))
                .clean()
                .getDeletedFilesPath();
    }

    private static Path bucketPath(FileStoreTable table) {
        return table.store().pathFactory().bucketPath(BinaryRow.EMPTY_ROW, 0);
    }

    private static Set<String> livePackNames(FileStoreTable table) throws IOException {
        Set<String> names = new HashSet<>();
        FileIO fileIO = table.fileIO();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            DataFilePathFactory pathFactory = factories.get(entry.partition(), entry.bucket());
            DataFileMeta file = entry.file();
            for (String extra : file.extraFiles()) {
                if (!extra.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
                    continue;
                }
                Path sidecar = pathFactory.toAlignedPath(extra, file);
                for (ManagedBlobReferenceFile.Reference ref :
                        ManagedBlobReferenceFile.read(fileIO, sidecar)) {
                    names.add(ref.relativePath());
                }
            }
        }
        return names;
    }

    private static List<Path> managedBlobs(FileStoreTable table) throws IOException {
        List<Path> packs = new ArrayList<>();
        FileStatus[] statuses = table.fileIO().listStatus(bucketPath(table));
        if (statuses == null) {
            return packs;
        }
        for (FileStatus status : statuses) {
            if (status.getPath().getName().endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)) {
                packs.add(status.getPath());
            }
        }
        return packs;
    }

    private static StaleScan collectUsedPacks(
            FileStoreTable table, Iterable<Snapshot> snapshots) throws IOException {
        StaleScan scan = new StaleScan();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());
        ManagedBlobReachabilityCollector collector =
                new ManagedBlobReachabilityCollector(table.fileIO());
        for (Snapshot snapshot : snapshots) {
            List<ManifestFileMeta> metas;
            try {
                metas = manifestList.readDataManifests(snapshot);
            } catch (Exception e) {
                scan.skip = true;
                return scan;
            }
            for (ManifestFileMeta meta : metas) {
                List<ManifestEntry> entries;
                try {
                    entries = manifestFile.read(meta.fileName());
                } catch (Exception e) {
                    scan.skip = true;
                    return scan;
                }
                for (ManifestEntry entry : entries) {
                    if (entry.kind() != FileKind.ADD) {
                        continue;
                    }
                    Result result =
                            collector.fromDataFile(
                                    factories.get(entry.partition(), entry.bucket())
                                            .toPath(entry),
                                    entry.file().extraFiles());
                    if (result.isUnsafe()) {
                        scan.skip = true;
                        return scan;
                    }
                    for (Reference reference : result.referenced()) {
                        scan.packs.add(reference.relativePath());
                    }
                }
            }
        }
        return scan;
    }

    private static final class StaleScan {
        private boolean skip;
        private final Set<String> packs = new HashSet<>();
    }

    private static void deleteSidecars(FileStoreTable table) throws IOException {
        FileIO fileIO = table.fileIO();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            DataFilePathFactory pathFactory = factories.get(entry.partition(), entry.bucket());
            DataFileMeta file = entry.file();
            for (String extra : file.extraFiles()) {
                if (extra.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
                    fileIO.deleteQuietly(pathFactory.toAlignedPath(extra, file));
                }
            }
        }
    }

    private interface SidecarOverwriter {
        void write(DataOutputStream out) throws IOException;
    }

    private static void overwriteSidecars(FileStoreTable table, SidecarOverwriter overwriter)
            throws IOException {
        FileIO fileIO = table.fileIO();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            DataFilePathFactory pathFactory = factories.get(entry.partition(), entry.bucket());
            DataFileMeta file = entry.file();
            for (String extra : file.extraFiles()) {
                if (!extra.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
                    continue;
                }
                Path sidecar = pathFactory.toAlignedPath(extra, file);
                fileIO.deleteQuietly(sidecar);
                try (DataOutputStream out =
                        new DataOutputStream(fileIO.newOutputStream(sidecar, false))) {
                    overwriter.write(out);
                }
            }
        }
    }
}
