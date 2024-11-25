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

import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.ThreadPoolUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DeletionVectorsMaintainer}. */
public class DeletionVectorsMaintainerTest extends PrimaryKeyTableTestBase {
    private IndexFileHandler fileHandler;

    @BeforeEach
    public void beforeEach() throws Exception {
        fileHandler = table.store().newIndexFileHandler();
    }

    @Test
    public void test0() {
        DeletionVectorsMaintainer.Factory factory =
                new DeletionVectorsMaintainer.Factory(fileHandler);
        DeletionVectorsMaintainer dvMaintainer =
                factory.createOrRestore(null, BinaryRow.EMPTY_ROW, 0);

        dvMaintainer.notifyNewDeletion("f1", 1);
        dvMaintainer.notifyNewDeletion("f2", 2);
        dvMaintainer.notifyNewDeletion("f3", 3);
        dvMaintainer.removeDeletionVectorOf("f3");

        assertThat(dvMaintainer.deletionVectorOf("f1")).isPresent();
        assertThat(dvMaintainer.deletionVectorOf("f3")).isEmpty();
        List<IndexFileMeta> fileMetas = dvMaintainer.writeDeletionVectorsIndex();

        Map<String, DeletionVector> deletionVectors = fileHandler.readAllDeletionVectors(fileMetas);
        assertThat(deletionVectors.get("f1").isDeleted(1)).isTrue();
        assertThat(deletionVectors.get("f1").isDeleted(2)).isFalse();
        assertThat(deletionVectors.get("f2").isDeleted(1)).isFalse();
        assertThat(deletionVectors.get("f2").isDeleted(2)).isTrue();
        assertThat(deletionVectors.containsKey("f3")).isFalse();
    }

    @Test
    public void test1() {
        DeletionVectorsMaintainer.Factory factory =
                new DeletionVectorsMaintainer.Factory(fileHandler);

        DeletionVectorsMaintainer dvMaintainer = factory.create();
        BitmapDeletionVector deletionVector1 = new BitmapDeletionVector();
        deletionVector1.delete(1);
        deletionVector1.delete(3);
        deletionVector1.delete(5);
        dvMaintainer.notifyNewDeletion("f1", deletionVector1);

        List<IndexFileMeta> fileMetas1 = dvMaintainer.writeDeletionVectorsIndex();
        assertThat(fileMetas1.size()).isEqualTo(1);
        CommitMessage commitMessage =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        DataIncrement.emptyIncrement(),
                        CompactIncrement.emptyIncrement(),
                        new IndexIncrement(fileMetas1));
        BatchTableCommit commit = table.newBatchWriteBuilder().newCommit();
        commit.commit(Collections.singletonList(commitMessage));

        Long lastSnapshotId = table.snapshotManager().latestSnapshotId();
        dvMaintainer = factory.createOrRestore(lastSnapshotId, BinaryRow.EMPTY_ROW, 0);
        DeletionVector deletionVector2 = dvMaintainer.deletionVectorOf("f1").get();
        assertThat(deletionVector2.isDeleted(1)).isTrue();
        assertThat(deletionVector2.isDeleted(2)).isFalse();

        deletionVector2.delete(2);
        dvMaintainer.notifyNewDeletion("f1", deletionVector2);

        List<IndexFileMeta> fileMetas2 = dvMaintainer.writeDeletionVectorsIndex();
        assertThat(fileMetas2.size()).isEqualTo(1);
        commitMessage =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        DataIncrement.emptyIncrement(),
                        CompactIncrement.emptyIncrement(),
                        new IndexIncrement(fileMetas2));
        commit = table.newBatchWriteBuilder().newCommit();
        commit.commit(Collections.singletonList(commitMessage));

        lastSnapshotId = table.snapshotManager().latestSnapshotId();
        dvMaintainer = factory.createOrRestore(lastSnapshotId, BinaryRow.EMPTY_ROW, 0);
        DeletionVector deletionVector3 = dvMaintainer.deletionVectorOf("f1").get();
        assertThat(deletionVector3.isDeleted(1)).isTrue();
        assertThat(deletionVector3.isDeleted(2)).isTrue();
    }

    @Test
    public void testCompactDeletion() throws IOException {
        DeletionVectorsMaintainer.Factory factory =
                new DeletionVectorsMaintainer.Factory(fileHandler);
        DeletionVectorsMaintainer dvMaintainer =
                factory.createOrRestore(null, BinaryRow.EMPTY_ROW, 0);

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

    @Test
    void testParallelNotifyNewDeletionAndWriteIndex() {
        DeletionVectorsMaintainer.Factory factory =
                new DeletionVectorsMaintainer.Factory(fileHandler);
        DeletionVectorsMaintainer dvMaintainer =
                factory.createOrRestore(null, BinaryRow.EMPTY_ROW, 0);

        ThreadPoolExecutor threadPool = ThreadPoolUtils.createCachedThreadPool(2, "dv-");
        try {
            Runnable delete =
                    () -> {
                        for (int i = 0; i < 100; i++) {
                            dvMaintainer.notifyNewDeletion("f" + i, i);
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };

            Runnable compact =
                    () -> {
                        for (int i = 0; i < 10; i++) {
                            dvMaintainer.writeDeletionVectorsIndex();
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };

            Future<?> deleteFuture = threadPool.submit(delete);
            Future<?> compactFuture = threadPool.submit(compact);

            try {
                compactFuture.get();
                deleteFuture.get();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }

            dvMaintainer.notifyNewDeletion("f101", 101);
            List<IndexFileMeta> indexFileMetas = dvMaintainer.writeDeletionVectorsIndex();
            assertThat(indexFileMetas.size()).isEqualTo(1);
            assertThat(indexFileMetas.get(0).rowCount()).isEqualTo(101);
        } finally {
            threadPool.shutdown();
        }
    }
}
