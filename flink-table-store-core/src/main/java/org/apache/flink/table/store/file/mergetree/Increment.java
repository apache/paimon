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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.table.store.file.data.DataFileMeta;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Incremental files for merge tree. It consists of two parts:
 *
 * <ul>
 *   <li>New files: The new files generated in this snapshot cycle. They must be committed.
 *   <li>Compact files: The {@link #compactBefore} files are compacted to {@link #compactAfter}
 *       files in this snapshot cycle. The compaction is an optimization of files.
 * </ul>
 */
public class Increment {

    private static final List<DataFileMeta> EMPTY_NEW_FILES = Collections.emptyList();
    private static final List<DataFileMeta> EMPTY_COMPACT_BEFORE = Collections.emptyList();
    private static final List<DataFileMeta> EMPTY_COMPACT_AFTER = Collections.emptyList();

    private final List<DataFileMeta> newFiles;

    private final List<DataFileMeta> compactBefore;

    private final List<DataFileMeta> compactAfter;

    public static Increment forAppend(List<DataFileMeta> newFiles) {
        return new Increment(newFiles, EMPTY_COMPACT_BEFORE, EMPTY_COMPACT_AFTER);
    }

    public static Increment forCompact(
            List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        return new Increment(EMPTY_NEW_FILES, compactBefore, compactAfter);
    }

    public Increment(
            List<DataFileMeta> newFiles,
            List<DataFileMeta> beCompacted,
            List<DataFileMeta> compacted) {
        this.newFiles = Collections.unmodifiableList(newFiles);
        this.compactBefore = Collections.unmodifiableList(beCompacted);
        this.compactAfter = Collections.unmodifiableList(compacted);
    }

    public List<DataFileMeta> newFiles() {
        return newFiles;
    }

    public List<DataFileMeta> compactBefore() {
        return compactBefore;
    }

    public List<DataFileMeta> compactAfter() {
        return compactAfter;
    }

    public boolean isEmpty() {
        return newFiles.isEmpty() && compactBefore.isEmpty() && compactAfter.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Increment increment = (Increment) o;
        return Objects.equals(newFiles, increment.newFiles)
                && Objects.equals(compactBefore, increment.compactBefore)
                && Objects.equals(compactAfter, increment.compactAfter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(newFiles, compactBefore, compactAfter);
    }

    @Override
    public String toString() {
        return "Increment{"
                + "newFiles="
                + newFiles
                + ", compactBefore="
                + compactBefore
                + ", compactAfter="
                + compactAfter
                + '}';
    }
}
