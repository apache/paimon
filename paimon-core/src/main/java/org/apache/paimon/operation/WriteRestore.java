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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;

import javax.annotation.Nullable;

import java.util.List;

/** Restore for write to restore data files by partition and bucket from file system. */
public interface WriteRestore {

    long latestCommittedIdentifier(String user);

    RestoreFiles restoreFiles(
            BinaryRow partition,
            int bucket,
            boolean scanDynamicBucketIndex,
            boolean scanDeleteVectorsIndex);

    @Nullable
    static Integer extractDataFiles(List<ManifestEntry> entries, List<DataFileMeta> dataFiles) {
        Integer totalBuckets = null;
        for (ManifestEntry entry : entries) {
            if (totalBuckets != null && totalBuckets != entry.totalBuckets()) {
                throw new RuntimeException(
                        String.format(
                                "Bucket data files has different total bucket number, %s vs %s, this should be a bug.",
                                totalBuckets, entry.totalBuckets()));
            }
            totalBuckets = entry.totalBuckets();
            dataFiles.add(entry.file());
        }
        return totalBuckets;
    }
}
