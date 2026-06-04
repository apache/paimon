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

import org.apache.paimon.manifest.ManifestFileMeta;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@code ManifestAdjacentSortedRun} is a list of {@link ManifestFileMeta}s sorted by a single
 * partition field (the configured manifest sort field). The intervals {@code
 * [partitionStats.minValues[k], partitionStats.maxValues[k]]} of these manifests do not overlap on
 * field {@code k}, where {@code k} is the configured sort field index.
 *
 * <p><b>Boundary Equality:</b> Files with boundary-touching intervals (min == previous.max) are
 * considered non-overlapping and can be placed in the same SortedRun. This reduces the number of
 * runs and improves compaction efficiency. However, such files may be separated into different
 * Sections during splitIntoSections to avoid merge-sort overhead.
 */
public class ManifestAdjacentSortedRun {

    private int level;
    private final List<ManifestFileMeta> files;
    private final long totalSize;

    private ManifestAdjacentSortedRun(List<ManifestFileMeta> files) {
        this.level = -1;
        this.files = Collections.unmodifiableList(files);
        long size = 0L;
        for (ManifestFileMeta file : files) {
            size += file.fileSize();
        }
        this.totalSize = size;
    }

    /**
     * Build a {@code ManifestAdjacentSortedRun} from an already-sorted list. The caller MUST
     * guarantee that {@code sortedFiles} is sorted ascending on the configured sort field's min
     * value, and that intervals do not overlap on that field.
     */
    public static ManifestAdjacentSortedRun fromSorted(List<ManifestFileMeta> sortedFiles) {
        return new ManifestAdjacentSortedRun(sortedFiles);
    }

    public List<ManifestFileMeta> files() {
        return files;
    }

    public long totalSize() {
        return totalSize;
    }

    public int level() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestAdjacentSortedRun)) {
            return false;
        }
        ManifestAdjacentSortedRun that = (ManifestAdjacentSortedRun) o;
        return level == that.level && files.equals(that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, files);
    }

    @Override
    public String toString() {
        return "ManifestAdjacentSortedRun{level="
                + level
                + ", files=["
                + files.stream().map(ManifestFileMeta::fileName).collect(Collectors.joining(", "))
                + "]}";
    }
}
