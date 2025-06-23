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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.source.DeletionFile;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;

/**
 * A maintainer to maintain deletion files for append table, the core methods:
 *
 * <ul>
 *   <li>{@link #notifyNewDeletionVector}: Mark the deletion of data files, create new deletion
 *       vectors.
 *   <li>{@link #persist}: persist deletion files to commit.
 * </ul>
 */
public interface BaseAppendDeleteFileMaintainer {

    BinaryRow getPartition();

    int getBucket();

    void notifyNewDeletionVector(String dataFile, DeletionVector deletionVector);

    List<IndexManifestEntry> persist();

    static BucketedAppendDeleteFileMaintainer forBucketedAppend(
            IndexFileHandler indexFileHandler,
            @Nullable Snapshot snapshot,
            BinaryRow partition,
            int bucket) {
        // bucket should have only one deletion file, so here we should read old deletion vectors,
        // overwrite the entire deletion file of the bucket when writing deletes.
        List<IndexFileMeta> indexFiles =
                indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX, partition, bucket);
        DeletionVectorsMaintainer maintainer =
                new DeletionVectorsMaintainer.Factory(indexFileHandler).create(indexFiles);
        return new BucketedAppendDeleteFileMaintainer(partition, bucket, maintainer);
    }

    static AppendDeleteFileMaintainer forUnawareAppend(
            IndexFileHandler indexFileHandler, @Nullable Snapshot snapshot, BinaryRow partition) {
        Map<String, DeletionFile> deletionFiles =
                indexFileHandler.scanDVIndex(snapshot, partition, UNAWARE_BUCKET);
        return new AppendDeleteFileMaintainer(indexFileHandler, partition, deletionFiles);
    }
}
