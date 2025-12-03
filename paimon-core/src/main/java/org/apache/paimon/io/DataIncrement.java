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

package org.apache.paimon.io;

import org.apache.paimon.index.IndexFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Increment of data files, changelog files and index files. */
public class DataIncrement {

    private final List<DataFileMeta> newFiles;
    private final List<DataFileMeta> deletedFiles;
    private final List<DataFileMeta> changelogFiles;
    private final List<IndexFileMeta> newIndexFiles;
    private final List<IndexFileMeta> deletedIndexFiles;

    public DataIncrement(
            List<DataFileMeta> newFiles,
            List<DataFileMeta> deletedFiles,
            List<DataFileMeta> changelogFiles) {
        this(newFiles, deletedFiles, changelogFiles, new ArrayList<>(), new ArrayList<>());
    }

    public DataIncrement(
            List<DataFileMeta> newFiles,
            List<DataFileMeta> deletedFiles,
            List<DataFileMeta> changelogFiles,
            List<IndexFileMeta> newIndexFiles,
            List<IndexFileMeta> deletedIndexFiles) {
        this.newFiles = newFiles;
        this.deletedFiles = deletedFiles;
        this.changelogFiles = changelogFiles;
        this.newIndexFiles = newIndexFiles;
        this.deletedIndexFiles = deletedIndexFiles;
    }

    public static DataIncrement emptyIncrement() {
        return new DataIncrement(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    public static DataIncrement indexIncrement(List<IndexFileMeta> indexFiles) {
        return new DataIncrement(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                indexFiles,
                Collections.emptyList());
    }

    public List<DataFileMeta> newFiles() {
        return newFiles;
    }

    public List<DataFileMeta> deletedFiles() {
        return deletedFiles;
    }

    public List<DataFileMeta> changelogFiles() {
        return changelogFiles;
    }

    public List<IndexFileMeta> newIndexFiles() {
        return newIndexFiles;
    }

    public List<IndexFileMeta> deletedIndexFiles() {
        return deletedIndexFiles;
    }

    public boolean isEmpty() {
        return newFiles.isEmpty()
                && deletedFiles.isEmpty()
                && changelogFiles.isEmpty()
                && newIndexFiles.isEmpty()
                && deletedIndexFiles.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataIncrement that = (DataIncrement) o;
        return Objects.equals(newFiles, that.newFiles)
                && Objects.equals(deletedFiles, that.deletedFiles)
                && Objects.equals(changelogFiles, that.changelogFiles)
                && Objects.equals(newIndexFiles, that.newIndexFiles)
                && Objects.equals(deletedIndexFiles, that.deletedIndexFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                newFiles, deletedFiles, changelogFiles, newIndexFiles, deletedIndexFiles);
    }

    @Override
    public String toString() {
        return String.format(
                "DataIncrement {newFiles = %s, deletedFiles = %s, changelogFiles = %s, newIndexFiles = %s, deletedIndexFiles = %s}",
                newFiles.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                deletedFiles.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                changelogFiles.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                newIndexFiles.stream().map(IndexFileMeta::fileName).collect(Collectors.toList()),
                deletedIndexFiles.stream()
                        .map(IndexFileMeta::fileName)
                        .collect(Collectors.toList()));
    }
}
