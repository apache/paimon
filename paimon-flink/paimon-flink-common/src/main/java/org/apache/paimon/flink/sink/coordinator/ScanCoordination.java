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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.Snapshot;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;

import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Scan coordination to initial data files for partition and bucket. */
public class ScanCoordination implements CoordinationResponse {

    private static final long serialVersionUID = 1L;

    @Nullable private final Snapshot snapshot;
    @Nullable private final Integer totalBuckets;
    @Nullable private final List<byte[]> dataFiles;
    @Nullable private final byte[] dynamicBucketIndex;
    @Nullable private final List<byte[]> deleteVectorsIndex;

    public ScanCoordination(
            @Nullable Snapshot snapshot,
            @Nullable Integer totalBuckets,
            @Nullable List<DataFileMeta> dataFiles,
            @Nullable IndexFileMeta dynamicBucketIndex,
            @Nullable List<IndexFileMeta> deleteVectorsIndex)
            throws IOException {
        this.snapshot = snapshot;
        this.totalBuckets = totalBuckets;

        DataFileMetaSerializer serializer = new DataFileMetaSerializer();
        if (dataFiles != null) {
            this.dataFiles = new ArrayList<>(dataFiles.size());
            for (DataFileMeta dataFile : dataFiles) {
                this.dataFiles.add(serializer.serializeToBytes(dataFile));
            }
        } else {
            this.dataFiles = null;
        }

        IndexFileMetaSerializer indexSerializer = new IndexFileMetaSerializer();
        if (dynamicBucketIndex != null) {
            this.dynamicBucketIndex = indexSerializer.serializeToBytes(dynamicBucketIndex);
        } else {
            this.dynamicBucketIndex = null;
        }

        if (deleteVectorsIndex != null) {
            this.deleteVectorsIndex = new ArrayList<>(deleteVectorsIndex.size());
            for (IndexFileMeta indexFile : deleteVectorsIndex) {
                this.deleteVectorsIndex.add(indexSerializer.serializeToBytes(indexFile));
            }
        } else {
            this.deleteVectorsIndex = null;
        }
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
    public List<DataFileMeta> extractDataFiles() throws IOException {
        if (dataFiles == null) {
            return null;
        }
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();
        List<DataFileMeta> dataFileMetas = new ArrayList<>(dataFiles.size());
        for (byte[] file : dataFiles) {
            dataFileMetas.add(serializer.deserializeFromBytes(file));
        }
        return dataFileMetas;
    }

    @Nullable
    public IndexFileMeta extractDynamicBucketIndex() throws IOException {
        if (dynamicBucketIndex == null) {
            return null;
        }
        return new IndexFileMetaSerializer().deserializeFromBytes(dynamicBucketIndex);
    }

    @Nullable
    public List<IndexFileMeta> extractDeletionVectorsIndex() throws IOException {
        if (deleteVectorsIndex == null) {
            return null;
        }
        IndexFileMetaSerializer serializer = new IndexFileMetaSerializer();
        List<IndexFileMeta> metas = new ArrayList<>(deleteVectorsIndex.size());
        for (byte[] file : deleteVectorsIndex) {
            metas.add(serializer.deserializeFromBytes(file));
        }
        return metas;
    }
}
