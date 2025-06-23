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

package org.apache.paimon.flink.sink;

import org.apache.paimon.Snapshot;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;

import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Write response to initial data files for partition and bucket. */
public class ScanCoordinationResponse implements CoordinationResponse {

    private static final long serialVersionUID = 1L;

    @Nullable private final Snapshot snapshot;
    @Nullable private final List<byte[]> dataFiles;
    @Nullable private final Integer totalBuckets;

    public ScanCoordinationResponse(
            @Nullable Snapshot snapshot,
            @Nullable List<DataFileMeta> dataFiles,
            @Nullable Integer totalBuckets)
            throws IOException {
        this.snapshot = snapshot;
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();
        if (dataFiles != null) {
            this.dataFiles = new ArrayList<>(dataFiles.size());
            for (DataFileMeta dataFile : dataFiles) {
                this.dataFiles.add(serializer.serializeToBytes(dataFile));
            }
        } else {
            this.dataFiles = null;
        }
        this.totalBuckets = totalBuckets;
    }

    @Nullable
    public Snapshot snapshot() {
        return snapshot;
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
    public Integer totalBuckets() {
        return totalBuckets;
    }
}
