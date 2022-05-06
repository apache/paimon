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

package org.apache.flink.table.store.file.manifest;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.Increment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Manifest commit message. */
public class ManifestCommittable {

    private final String identifier;
    private final Map<Integer, Long> logOffsets;
    private final Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> newFiles;
    private final Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> compactBefore;
    private final Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> compactAfter;

    public ManifestCommittable(String identifier) {
        this.identifier = identifier;
        this.logOffsets = new HashMap<>();
        this.newFiles = new HashMap<>();
        this.compactBefore = new HashMap<>();
        this.compactAfter = new HashMap<>();
    }

    public ManifestCommittable(
            String identifier,
            Map<Integer, Long> logOffsets,
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> newFiles,
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> compactBefore,
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> compactAfter) {
        this.identifier = identifier;
        this.logOffsets = logOffsets;
        this.newFiles = newFiles;
        this.compactBefore = compactBefore;
        this.compactAfter = compactAfter;
    }

    public void addFileCommittable(BinaryRowData partition, int bucket, Increment increment) {
        addFiles(newFiles, partition, bucket, increment.newFiles());
        addFiles(compactBefore, partition, bucket, increment.compactBefore());
        addFiles(compactAfter, partition, bucket, increment.compactAfter());
    }

    public void addLogOffset(int bucket, long offset) {
        if (logOffsets.containsKey(bucket)) {
            throw new RuntimeException(
                    String.format(
                            "bucket-%d appears multiple times, which is not possible.", bucket));
        }
        logOffsets.put(bucket, offset);
    }

    private static void addFiles(
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> map,
            BinaryRowData partition,
            int bucket,
            List<DataFileMeta> files) {
        map.computeIfAbsent(partition, k -> new HashMap<>())
                .computeIfAbsent(bucket, k -> new ArrayList<>())
                .addAll(files);
    }

    public String identifier() {
        return identifier;
    }

    public Map<Integer, Long> logOffsets() {
        return logOffsets;
    }

    public Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> newFiles() {
        return newFiles;
    }

    public Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> compactBefore() {
        return compactBefore;
    }

    public Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> compactAfter() {
        return compactAfter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ManifestCommittable that = (ManifestCommittable) o;
        return Objects.equals(identifier, that.identifier)
                && Objects.equals(logOffsets, that.logOffsets)
                && Objects.equals(newFiles, that.newFiles)
                && Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, logOffsets, newFiles, compactBefore, compactAfter);
    }

    @Override
    public String toString() {
        return "ManifestCommittable { "
                + "identifier = "
                + identifier
                + ", logOffsets = "
                + logOffsets
                + ", newFiles =\n"
                + filesToString(newFiles)
                + ", compactBefore =\n"
                + filesToString(compactBefore)
                + ", compactAfter =\n"
                + filesToString(compactAfter)
                + '}';
    }

    private static String filesToString(
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> files) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> entryWithPartition :
                files.entrySet()) {
            for (Map.Entry<Integer, List<DataFileMeta>> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                for (DataFileMeta dataFile : entryWithBucket.getValue()) {
                    builder.append("  * partition: ")
                            .append(entryWithPartition.getKey())
                            .append(", bucket: ")
                            .append(entryWithBucket.getKey())
                            .append(", file: ")
                            .append(dataFile.fileName())
                            .append("\n");
                }
            }
        }
        return builder.toString();
    }
}
