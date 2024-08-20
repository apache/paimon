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
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.source.DeletionFile;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** A {@link AppendDeletionFileMaintainer} of bucketed append table. */
public class BucketedAppendDeletionFileMaintainer implements AppendDeletionFileMaintainer {

    private final BinaryRow partition;
    private final int bucket;
    private final Map<String, DeletionFile> dataFileToDeletionFile;
    private final DeletionVectorsMaintainer maintainer;

    BucketedAppendDeletionFileMaintainer(
            BinaryRow partition,
            int bucket,
            Map<String, DeletionFile> deletionFiles,
            DeletionVectorsMaintainer maintainer) {
        this.partition = partition;
        this.bucket = bucket;
        this.dataFileToDeletionFile = deletionFiles;
        this.maintainer = maintainer;
    }

    @Override
    public BinaryRow getPartition() {
        return this.partition;
    }

    @Override
    public int getBucket() {
        return this.bucket;
    }

    @Override
    public DeletionFile getDeletionFile(String dataFile) {
        return dataFileToDeletionFile.get(dataFile);
    }

    @Override
    public DeletionVector getDeletionVector(String dataFile) {
        return this.maintainer.deletionVectorOf(dataFile).orElse(null);
    }

    @Override
    public void notifyNewDeletionVector(String dataFile, DeletionVector deletionVector) {
        maintainer.mergeNewDeletion(dataFile, deletionVector);
    }

    @Override
    public void notifyRemovedDeletionVector(String dataFile) {
        maintainer.removeDeletionVectorOf(dataFile);
    }

    @Override
    public List<IndexManifestEntry> persist() {
        return maintainer.writeDeletionVectorsIndex().stream()
                .map(fileMeta -> new IndexManifestEntry(FileKind.ADD, partition, bucket, fileMeta))
                .collect(Collectors.toList());
    }
}
