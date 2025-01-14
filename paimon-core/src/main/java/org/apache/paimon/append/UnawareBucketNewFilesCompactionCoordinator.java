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

package org.apache.paimon.append;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Buffer files from the same partition, until their total size reaches {@code targetFileSize}. */
public class UnawareBucketNewFilesCompactionCoordinator {

    private final long targetFileSize;
    private final Map<BinaryRow, PartitionFiles> partitions;

    public UnawareBucketNewFilesCompactionCoordinator(long targetFileSize) {
        this.targetFileSize = targetFileSize;
        this.partitions = new LinkedHashMap<>();
    }

    public Optional<Pair<BinaryRow, List<DataFileMeta>>> addFile(
            BinaryRow partition, DataFileMeta file) {
        PartitionFiles files =
                partitions.computeIfAbsent(partition, ignore -> new PartitionFiles());
        files.addFile(file);
        if (files.totalSize >= targetFileSize) {
            partitions.remove(partition);
            return Optional.of(Pair.of(partition, files.files));
        } else {
            return Optional.empty();
        }
    }

    public List<Pair<BinaryRow, List<DataFileMeta>>> emitAll() {
        List<Pair<BinaryRow, List<DataFileMeta>>> result =
                partitions.entrySet().stream()
                        .map(e -> Pair.of(e.getKey(), e.getValue().files))
                        .collect(Collectors.toList());
        partitions.clear();
        return result;
    }

    private static class PartitionFiles {
        private final List<DataFileMeta> files = new ArrayList<>();
        private long totalSize = 0;

        private void addFile(DataFileMeta file) {
            files.add(file);
            totalSize += file.fileSize();
        }
    }
}
