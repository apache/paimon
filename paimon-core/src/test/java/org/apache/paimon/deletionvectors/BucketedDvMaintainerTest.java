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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.FileIOUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BucketedDvMaintainer}. */
public class BucketedDvMaintainerTest extends PrimaryKeyTableTestBase {
    private IndexFileHandler fileHandler;
    private final BinaryRow partition = BinaryRow.singleColumn(1);

    @BeforeEach
    public void setUp() throws Exception {
        // write files
        CommitMessageImpl commitMessage =
                writeDataFiles(partition, 0, Arrays.asList("f1", "f2", "f3"));
        BatchTableCommit commit = table.newBatchWriteBuilder().newCommit();
        commit.commit(Collections.singletonList(commitMessage));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void test0(boolean bitmap64) {
        initIndexHandler(bitmap64);

        BucketedDvMaintainer.Factory factory = BucketedDvMaintainer.factory(fileHandler);
        BucketedDvMaintainer dvMaintainer = factory.create(partition, 0, emptyList());
        assertThat(dvMaintainer.bitmap64).isEqualTo(bitmap64);

        dvMaintainer.notifyNewDeletion("f1", 1);
        dvMaintainer.notifyNewDeletion("f2", 2);
        dvMaintainer.notifyNewDeletion("f3", 3);
        dvMaintainer.removeDeletionVectorOf("f3");

        assertThat(dvMaintainer.deletionVectorOf("f1")).isPresent();
        assertThat(dvMaintainer.deletionVectorOf("f3")).isEmpty();
        IndexFileMeta file = dvMaintainer.writeDeletionVectorsIndex().get();

        Map<String, DeletionVector> deletionVectors =
                fileHandler.readAllDeletionVectors(partition, 0, Collections.singletonList(file));
        assertThat(deletionVectors.get("f1").isDeleted(1)).isTrue();
        assertThat(deletionVectors.get("f1").isDeleted(2)).isFalse();
        assertThat(deletionVectors.get("f2").isDeleted(1)).isFalse();
        assertThat(deletionVectors.get("f2").isDeleted(2)).isTrue();
        assertThat(deletionVectors.containsKey("f3")).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void test1(boolean bitmap64) {
        initIndexHandler(bitmap64);

        BucketedDvMaintainer.Factory factory = BucketedDvMaintainer.factory(fileHandler);

        BucketedDvMaintainer dvMaintainer = factory.create(partition, 0, new HashMap<>());
        DeletionVector deletionVector1 = createDeletionVector(bitmap64);
        deletionVector1.delete(1);
        deletionVector1.delete(3);
        deletionVector1.delete(5);
        dvMaintainer.notifyNewDeletion("f1", deletionVector1);
        assertThat(dvMaintainer.bitmap64()).isEqualTo(bitmap64);

        IndexFileMeta file = dvMaintainer.writeDeletionVectorsIndex().get();
        CommitMessage commitMessage =
                new CommitMessageImpl(
                        partition,
                        0,
                        1,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.singletonList(file),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        BatchTableCommit commit = table.newBatchWriteBuilder().newCommit();
        commit.commit(Collections.singletonList(commitMessage));

        Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
        List<IndexFileMeta> indexFiles =
                fileHandler.scan(latestSnapshot, DELETION_VECTORS_INDEX, partition, 0);
        dvMaintainer = factory.create(partition, 0, indexFiles);
        DeletionVector deletionVector2 = dvMaintainer.deletionVectorOf("f1").get();
        assertThat(deletionVector2.isDeleted(1)).isTrue();
        assertThat(deletionVector2.isDeleted(2)).isFalse();

        deletionVector2.delete(2);
        dvMaintainer.notifyNewDeletion("f1", deletionVector2);

        file = dvMaintainer.writeDeletionVectorsIndex().get();
        commitMessage =
                new CommitMessageImpl(
                        partition,
                        0,
                        1,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.singletonList(file),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        commit = table.newBatchWriteBuilder().newCommit();
        commit.commit(Collections.singletonList(commitMessage));

        latestSnapshot = table.snapshotManager().latestSnapshot();
        indexFiles = fileHandler.scan(latestSnapshot, DELETION_VECTORS_INDEX, partition, 0);
        dvMaintainer = factory.create(partition, 0, indexFiles);
        DeletionVector deletionVector3 = dvMaintainer.deletionVectorOf("f1").get();
        assertThat(deletionVector3.isDeleted(1)).isTrue();
        assertThat(deletionVector3.isDeleted(2)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCompactDeletion(boolean bitmap64) throws IOException {
        initIndexHandler(bitmap64);

        BucketedDvMaintainer.Factory factory = BucketedDvMaintainer.factory(fileHandler);
        BucketedDvMaintainer dvMaintainer = factory.create(partition, 0, emptyList());

        File indexDir = new File(tempPath.toFile(), "/default.db/T/index");

        // test generate files

        dvMaintainer.notifyNewDeletion("f1", 1);
        CompactDeletionFile deletionFile1 = CompactDeletionFile.generateFiles(dvMaintainer);
        assertThat(indexDir.listFiles()).hasSize(1);

        dvMaintainer.notifyNewDeletion("f2", 4);
        CompactDeletionFile deletionFile2 = CompactDeletionFile.generateFiles(dvMaintainer);
        assertThat(indexDir.listFiles()).hasSize(2);

        deletionFile2.mergeOldFile(deletionFile1);
        assertThat(indexDir.listFiles()).hasSize(1);
        FileIOUtils.deleteDirectory(indexDir);

        // test lazyGeneration

        dvMaintainer.notifyNewDeletion("f1", 3);
        CompactDeletionFile deletionFile3 = CompactDeletionFile.lazyGeneration(dvMaintainer);
        assertThat(indexDir.listFiles()).isNull();

        dvMaintainer.notifyNewDeletion("f2", 5);
        CompactDeletionFile deletionFile4 = CompactDeletionFile.lazyGeneration(dvMaintainer);
        assertThat(indexDir.listFiles()).isNull();

        deletionFile4.mergeOldFile(deletionFile3);
        assertThat(indexDir.listFiles()).isNull();

        deletionFile4.getOrCompute();
        assertThat(indexDir.listFiles()).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReadAndWriteMixedDv(boolean bitmap64) {
        // write first kind dv
        initIndexHandler(bitmap64);
        BucketedDvMaintainer.Factory factory1 = BucketedDvMaintainer.factory(fileHandler);
        BucketedDvMaintainer dvMaintainer1 = factory1.create(partition, 0, new HashMap<>());
        dvMaintainer1.notifyNewDeletion("f1", 1);
        dvMaintainer1.notifyNewDeletion("f1", 3);
        dvMaintainer1.notifyNewDeletion("f2", 1);
        dvMaintainer1.notifyNewDeletion("f2", 3);
        assertThat(dvMaintainer1.bitmap64()).isEqualTo(bitmap64);

        IndexFileMeta file = dvMaintainer1.writeDeletionVectorsIndex().get();
        CommitMessage commitMessage1 =
                new CommitMessageImpl(
                        partition,
                        0,
                        1,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.singletonList(file),
                                Collections.emptyList()));
        BatchTableCommit commit1 = table.newBatchWriteBuilder().newCommit();
        commit1.commit(Collections.singletonList(commitMessage1));

        // write second kind dv
        initIndexHandler(!bitmap64);
        BucketedDvMaintainer.Factory factory2 = BucketedDvMaintainer.factory(fileHandler);
        List<IndexFileMeta> indexFiles =
                fileHandler.scan(
                        table.latestSnapshot().get(), DELETION_VECTORS_INDEX, partition, 0);
        BucketedDvMaintainer dvMaintainer2 = factory2.create(partition, 0, indexFiles);
        dvMaintainer2.notifyNewDeletion("f1", 10);
        dvMaintainer2.notifyNewDeletion("f3", 1);
        dvMaintainer2.notifyNewDeletion("f3", 3);
        assertThat(dvMaintainer2.bitmap64()).isEqualTo(!bitmap64);

        // verify two kinds of dv can exist in the same dv maintainer
        Map<String, DeletionVector> dvs = dvMaintainer2.deletionVectors();
        assertThat(dvs.size()).isEqualTo(3);
        assertThat(dvs.get("f1").getCardinality()).isEqualTo(3);
        assertThat(dvs.get("f2"))
                .isInstanceOf(bitmap64 ? Bitmap64DeletionVector.class : BitmapDeletionVector.class);
        assertThat(dvs.get("f3"))
                .isInstanceOf(bitmap64 ? BitmapDeletionVector.class : Bitmap64DeletionVector.class);

        file = dvMaintainer2.writeDeletionVectorsIndex().get();
        CommitMessage commitMessage2 =
                new CommitMessageImpl(
                        partition,
                        0,
                        1,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.singletonList(file),
                                Collections.emptyList()));
        BatchTableCommit commit2 = table.newBatchWriteBuilder().newCommit();
        commit2.commit(Collections.singletonList(commitMessage2));

        // test read dv index file which contains two kinds of dv
        Map<String, DeletionVector> readDvs =
                fileHandler.readAllDeletionVectors(
                        partition,
                        0,
                        fileHandler.scan(
                                table.latestSnapshot().get(), "DELETION_VECTORS", partition, 0));
        assertThat(readDvs.size()).isEqualTo(3);
        assertThat(dvs.get("f1").getCardinality()).isEqualTo(3);
        assertThat(dvs.get("f2").getCardinality()).isEqualTo(2);
        assertThat(dvs.get("f3").getCardinality()).isEqualTo(2);
    }

    private DeletionVector createDeletionVector(boolean bitmap64) {
        return bitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
    }

    private void initIndexHandler(boolean bitmap64) {
        Map<String, String> options = new HashMap<>();

        options.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), String.valueOf(bitmap64));

        table = table.copy(options);
        fileHandler = table.store().newIndexFileHandler();
    }

    public static BucketedDvMaintainer createOrRestore(
            BucketedDvMaintainer.Factory factory,
            @Nullable Snapshot snapshot,
            BinaryRow partition) {
        IndexFileHandler handler = factory.indexFileHandler();
        List<IndexFileMeta> indexFiles =
                snapshot == null
                        ? Collections.emptyList()
                        : handler.scanEntries(snapshot, DELETION_VECTORS_INDEX, partition).stream()
                                .map(IndexManifestEntry::indexFile)
                                .collect(Collectors.toList());
        Map<String, DeletionVector> deletionVectors =
                new HashMap<>(handler.readAllDeletionVectors(partition, 0, indexFiles));
        return factory.create(partition, 0, deletionVectors);
    }

    private CommitMessageImpl writeDataFiles(
            BinaryRow partition, int bucket, List<String> dataFileNames) throws IOException {
        List<DataFileMeta> fileMetas = new ArrayList<>();
        Path bucketPath = table.store().pathFactory().bucketPath(partition, bucket);
        for (String dataFileName : dataFileNames) {
            Path path = new Path(bucketPath, dataFileName);
            table.fileIO().newOutputStream(path, false).close();
            fileMetas.add(
                    DataFileMeta.forAppend(
                            path.getName(),
                            10L,
                            10L,
                            SimpleStats.EMPTY_STATS,
                            0L,
                            0L,
                            table.schema().id(),
                            Collections.emptyList(),
                            null,
                            null,
                            null,
                            null,
                            null,
                            null));
        }
        return new CommitMessageImpl(
                partition,
                bucket,
                null,
                new DataIncrement(fileMetas, Collections.emptyList(), Collections.emptyList()),
                CompactIncrement.emptyIncrement());
    }
}
