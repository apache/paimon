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
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.FileIOUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DeletionVectorsMaintainer}. */
public class DeletionVectorsMaintainerTest extends PrimaryKeyTableTestBase {
    private IndexFileHandler fileHandler;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void test0(boolean bitmap64) {
        initIndexHandler(bitmap64);

        DeletionVectorsMaintainer.Factory factory =
                new DeletionVectorsMaintainer.Factory(fileHandler);
        DeletionVectorsMaintainer dvMaintainer =
                factory.createOrRestore(null, BinaryRow.EMPTY_ROW, 0);
        assertThat(dvMaintainer.bitmap64).isEqualTo(bitmap64);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void test1(boolean bitmap64) {
        initIndexHandler(bitmap64);

        DeletionVectorsMaintainer.Factory factory =
                new DeletionVectorsMaintainer.Factory(fileHandler);

        DeletionVectorsMaintainer dvMaintainer = factory.create();
        DeletionVector deletionVector1 = createDeletionVector(bitmap64);
        deletionVector1.delete(1);
        deletionVector1.delete(3);
        deletionVector1.delete(5);
        dvMaintainer.notifyNewDeletion("f1", deletionVector1);
        assertThat(dvMaintainer.bitmap64()).isEqualTo(bitmap64);

        List<IndexFileMeta> fileMetas1 = dvMaintainer.writeDeletionVectorsIndex();
        assertThat(fileMetas1.size()).isEqualTo(1);
        CommitMessage commitMessage =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        DataIncrement.emptyIncrement(),
                        CompactIncrement.emptyIncrement(),
                        new IndexIncrement(fileMetas1));
        BatchTableCommit commit = table.newBatchWriteBuilder().newCommit();
        commit.commit(Collections.singletonList(commitMessage));

        Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
        dvMaintainer = factory.createOrRestore(latestSnapshot, BinaryRow.EMPTY_ROW, 0);
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
                        1,
                        DataIncrement.emptyIncrement(),
                        CompactIncrement.emptyIncrement(),
                        new IndexIncrement(fileMetas2));
        commit = table.newBatchWriteBuilder().newCommit();
        commit.commit(Collections.singletonList(commitMessage));

        latestSnapshot = table.snapshotManager().latestSnapshot();
        dvMaintainer = factory.createOrRestore(latestSnapshot, BinaryRow.EMPTY_ROW, 0);
        DeletionVector deletionVector3 = dvMaintainer.deletionVectorOf("f1").get();
        assertThat(deletionVector3.isDeleted(1)).isTrue();
        assertThat(deletionVector3.isDeleted(2)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCompactDeletion(boolean bitmap64) throws IOException {
        initIndexHandler(bitmap64);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReadAndWriteMixedDv(boolean bitmap64) {
        // write first kind dv
        initIndexHandler(bitmap64);
        DeletionVectorsMaintainer.Factory factory1 =
                new DeletionVectorsMaintainer.Factory(fileHandler);
        DeletionVectorsMaintainer dvMaintainer1 = factory1.create();
        dvMaintainer1.notifyNewDeletion("f1", 1);
        dvMaintainer1.notifyNewDeletion("f1", 3);
        dvMaintainer1.notifyNewDeletion("f2", 1);
        dvMaintainer1.notifyNewDeletion("f2", 3);
        assertThat(dvMaintainer1.bitmap64()).isEqualTo(bitmap64);

        List<IndexFileMeta> fileMetas1 = dvMaintainer1.writeDeletionVectorsIndex();
        assertThat(fileMetas1.size()).isEqualTo(1);
        CommitMessage commitMessage1 =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        DataIncrement.emptyIncrement(),
                        CompactIncrement.emptyIncrement(),
                        new IndexIncrement(fileMetas1));
        BatchTableCommit commit1 = table.newBatchWriteBuilder().newCommit();
        commit1.commit(Collections.singletonList(commitMessage1));

        // write second kind dv
        initIndexHandler(!bitmap64);
        DeletionVectorsMaintainer.Factory factory2 =
                new DeletionVectorsMaintainer.Factory(fileHandler);
        DeletionVectorsMaintainer dvMaintainer2 =
                factory2.createOrRestore(table.latestSnapshot().get(), BinaryRow.EMPTY_ROW, 0);
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

        List<IndexFileMeta> fileMetas2 = dvMaintainer2.writeDeletionVectorsIndex();
        assertThat(fileMetas2.size()).isEqualTo(1);
        CommitMessage commitMessage2 =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        DataIncrement.emptyIncrement(),
                        CompactIncrement.emptyIncrement(),
                        new IndexIncrement(fileMetas2));
        BatchTableCommit commit2 = table.newBatchWriteBuilder().newCommit();
        commit2.commit(Collections.singletonList(commitMessage2));

        // test read dv index file which contains two kinds of dv
        Map<String, DeletionVector> readDvs =
                fileHandler.readAllDeletionVectors(
                        fileHandler.scan(
                                table.latestSnapshot().get(),
                                "DELETION_VECTORS",
                                BinaryRow.EMPTY_ROW,
                                0));
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
}
