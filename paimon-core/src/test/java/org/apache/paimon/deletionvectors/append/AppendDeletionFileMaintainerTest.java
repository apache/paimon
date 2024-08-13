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

package org.apache.paimon.deletionvectors.append;

import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AppendDeletionFileMaintainerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void test() throws Exception {
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        Map<String, List<Integer>> dvs = new HashMap<>();
        dvs.put("f1", Arrays.asList(1, 3, 5));
        dvs.put("f2", Arrays.asList(2, 4, 6));
        CommitMessageImpl commitMessage1 = store.writeDVIndexFiles(BinaryRow.EMPTY_ROW, 0, dvs);
        CommitMessageImpl commitMessage2 =
                store.writeDVIndexFiles(
                        BinaryRow.EMPTY_ROW,
                        1,
                        Collections.singletonMap("f3", Arrays.asList(1, 2, 3)));
        store.commit(commitMessage1, commitMessage2);

        PathFactory indexPathFactory = store.pathFactory().indexFileFactory();
        Map<String, DeletionFile> dataFileToDeletionFiles = new HashMap<>();
        dataFileToDeletionFiles.putAll(
                createDeletionFileMapFromIndexFileMetas(
                        indexPathFactory, commitMessage1.indexIncrement().newIndexFiles()));
        dataFileToDeletionFiles.putAll(
                createDeletionFileMapFromIndexFileMetas(
                        indexPathFactory, commitMessage2.indexIncrement().newIndexFiles()));

        UnawareAppendDeletionFileMaintainer dvIFMaintainer =
                store.createDVIFMaintainer(BinaryRow.EMPTY_ROW, dataFileToDeletionFiles);

        // no dv should be rewritten, because nothing is changed.
        List<IndexManifestEntry> res = dvIFMaintainer.writeUnchangedDeletionVector();
        assertThat(res.size()).isEqualTo(0);

        // the dv of f3 is updated, and the index file that contains the dv of f3 should be marked
        // as REMOVE.
        FileIO fileIO = LocalFileIO.create();
        dvIFMaintainer.notifyDeletionFiles(
                "f3", DeletionVector.read(fileIO, dataFileToDeletionFiles.get("f3")));
        res = dvIFMaintainer.writeUnchangedDeletionVector();
        assertThat(res.size()).isEqualTo(1);
        assertThat(res.get(0).kind()).isEqualTo(FileKind.DELETE);

        // the dv of f1 and f2 are in one index file, and the dv of f1 is updated.
        // the dv of f2 need to be rewritten, and this index file should be marked as REMOVE.
        dvIFMaintainer.notifyDeletionFiles(
                "f1", DeletionVector.read(fileIO, dataFileToDeletionFiles.get("f1")));
        res = dvIFMaintainer.writeUnchangedDeletionVector();
        assertThat(res.size()).isEqualTo(3);
        IndexManifestEntry entry =
                res.stream().filter(file -> file.kind() == FileKind.ADD).findAny().get();
        assertThat(entry.indexFile().deletionVectorsRanges().containsKey("f2")).isTrue();
        entry =
                res.stream()
                        .filter(file -> file.kind() == FileKind.DELETE)
                        .filter(file -> file.bucket() == 0)
                        .findAny()
                        .get();
        assertThat(entry.indexFile())
                .isEqualTo(commitMessage1.indexIncrement().newIndexFiles().get(0));
        entry =
                res.stream()
                        .filter(file -> file.kind() == FileKind.DELETE)
                        .filter(file -> file.bucket() == 1)
                        .findAny()
                        .get();
        assertThat(entry.indexFile())
                .isEqualTo(commitMessage2.indexIncrement().newIndexFiles().get(0));
    }

    private Map<String, DeletionFile> createDeletionFileMapFromIndexFileMetas(
            PathFactory indexPathFactory, List<IndexFileMeta> fileMetas) {
        Map<String, DeletionFile> dataFileToDeletionFiles = new HashMap<>();
        for (IndexFileMeta indexFileMeta : fileMetas) {
            for (Map.Entry<String, Pair<Integer, Integer>> range :
                    indexFileMeta.deletionVectorsRanges().entrySet()) {
                dataFileToDeletionFiles.put(
                        range.getKey(),
                        new DeletionFile(
                                indexPathFactory.toPath(indexFileMeta.fileName()).toString(),
                                range.getValue().getLeft(),
                                range.getValue().getRight()));
            }
        }
        return dataFileToDeletionFiles;
    }
}
