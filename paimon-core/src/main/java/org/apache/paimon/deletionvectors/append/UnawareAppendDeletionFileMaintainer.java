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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.source.DeletionFile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;

/** A {@link AppendDeletionFileMaintainer} of unaware bucket append table. */
public class UnawareAppendDeletionFileMaintainer extends AppendDeletionFileMaintainer {

    UnawareAppendDeletionFileMaintainer(
            IndexFileHandler indexFileHandler,
            BinaryRow partition,
            Map<String, DeletionFile> deletionFiles,
            DeletionVectorsMaintainer maintainer) {
        super(indexFileHandler, partition, deletionFiles, maintainer);
    }

    @Override
    public int getBucket() {
        return UNAWARE_BUCKET;
    }

    /**
     * In some operations like Update/Delete/MergeInto, a deletion vector of a data file will be
     * updated, then report the new one to update the state.
     */
    @Override
    public void notifyNewDeletionVector(String dataFile, DeletionVector deletionVector) {
        DeletionFile previous = notify(dataFile);
        if (previous != null) {
            DeletionVectorsIndexFile deletionVectorsIndexFile =
                    indexFileHandler.deletionVectorsIndex();
            deletionVector.merge(deletionVectorsIndexFile.readDeletionVector(dataFile, previous));
        }
        maintainer.notifyNewDeletion(dataFile, deletionVector);
    }

    @VisibleForTesting
    @Override
    List<IndexManifestEntry> writeUnchangedDeletionVector() {
        DeletionVectorsIndexFile deletionVectorsIndexFile = indexFileHandler.deletionVectorsIndex();
        List<IndexManifestEntry> newIndexEntries = new ArrayList<>();
        for (String indexFile : indexFileToDeletionFiles.keySet()) {
            if (touchedIndexFiles.contains(indexFile)) {
                IndexManifestEntry oldEntry = indexNameToEntry.get(indexFile);

                // write unchanged deletion vector.
                Map<String, DeletionFile> dataFileToDeletionFiles =
                        indexFileToDeletionFiles.get(indexFile);
                if (!dataFileToDeletionFiles.isEmpty()) {
                    List<IndexFileMeta> newIndexFiles =
                            deletionVectorsIndexFile.write(
                                    deletionVectorsIndexFile.readDeletionVector(
                                            dataFileToDeletionFiles));
                    newIndexFiles.forEach(
                            newIndexFile ->
                                    newIndexEntries.add(
                                            new IndexManifestEntry(
                                                    FileKind.ADD,
                                                    oldEntry.partition(),
                                                    oldEntry.bucket(),
                                                    newIndexFile)));
                }

                // mark the touched index file as removed.
                newIndexEntries.add(oldEntry.toDeleteEntry());
            }
        }
        return newIndexEntries;
    }
}
