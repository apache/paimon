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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.source.DeletionFile;

import java.util.*;

/** A {@link AppendDeletionFileMaintainer} of bucketed append table. */
public class BucketedAppendDeletionFileMaintainer extends AppendDeletionFileMaintainer {
    private final int bucket;

    BucketedAppendDeletionFileMaintainer(
            IndexFileHandler indexFileHandler,
            BinaryRow partition,
            int bucket,
            Map<String, DeletionFile> deletionFiles,
            DeletionVectorsMaintainer maintainer) {
        super(indexFileHandler, partition, deletionFiles, maintainer);
        this.bucket = bucket;
    }

    @Override
    public int getBucket() {
        return this.bucket;
    }

    @Override
    public void notifyNewDeletionVector(String dataFile, DeletionVector deletionVector) {
        maintainer.mergeNewDeletion(dataFile, deletionVector);
    }

    @Override
    List<IndexManifestEntry> writeUnchangedDeletionVector() {
        List<IndexManifestEntry> newIndexEntries = new ArrayList<>();
        for (String indexFile : indexFileToDeletionFiles.keySet()) {
            if (touchedIndexFiles.contains(indexFile)) {
                IndexManifestEntry oldEntry = indexNameToEntry.get(indexFile);
                // mark the touched index file as removed.
                newIndexEntries.add(oldEntry.toDeleteEntry());
            }
        }
        return newIndexEntries;
    }
}
