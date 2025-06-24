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

import org.apache.paimon.Snapshot;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;

import javax.annotation.Nullable;

import java.util.List;

/** Restored files with snapshot and total buckets. */
public class RestoreFiles {

    private final @Nullable Snapshot snapshot;
    private final @Nullable Integer totalBuckets;
    private final @Nullable List<DataFileMeta> dataFiles;
    private final @Nullable IndexFileMeta dynamicBucketIndex;
    private final @Nullable List<IndexFileMeta> deleteVectorsIndex;

    public RestoreFiles(
            @Nullable Snapshot snapshot,
            @Nullable Integer totalBuckets,
            @Nullable List<DataFileMeta> dataFiles,
            @Nullable IndexFileMeta dynamicBucketIndex,
            @Nullable List<IndexFileMeta> deleteVectorsIndex) {
        this.snapshot = snapshot;
        this.totalBuckets = totalBuckets;
        this.dataFiles = dataFiles;
        this.dynamicBucketIndex = dynamicBucketIndex;
        this.deleteVectorsIndex = deleteVectorsIndex;
    }

    @Nullable
    public Snapshot snapshot() {
        return snapshot;
    }

    @Nullable
    public Integer totalBuckets() {
        return totalBuckets;
    }

    @Nullable
    public List<DataFileMeta> dataFiles() {
        return dataFiles;
    }

    @Nullable
    public IndexFileMeta dynamicBucketIndex() {
        return dynamicBucketIndex;
    }

    @Nullable
    public List<IndexFileMeta> deleteVectorsIndex() {
        return deleteVectorsIndex;
    }

    public static RestoreFiles empty() {
        return new RestoreFiles(null, null, null, null, null);
    }
}
