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

/** Incremental index files. */
public class IndexIncrement {

    private final List<IndexFileMeta> newIndexFiles;

    private final List<IndexFileMeta> deletedIndexFiles;

    public IndexIncrement(List<IndexFileMeta> newIndexFiles) {
        this.newIndexFiles = newIndexFiles;
        this.deletedIndexFiles = Collections.emptyList();
    }

    public IndexIncrement(
            List<IndexFileMeta> newIndexFiles, List<IndexFileMeta> deletedIndexFiles) {
        this.newIndexFiles = newIndexFiles;
        this.deletedIndexFiles = deletedIndexFiles;
    }

    public List<IndexFileMeta> newIndexFiles() {
        return newIndexFiles;
    }

    public List<IndexFileMeta> deletedIndexFiles() {
        return deletedIndexFiles;
    }

    public boolean isEmpty() {
        return newIndexFiles.isEmpty() && deletedIndexFiles.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexIncrement that = (IndexIncrement) o;
        return Objects.equals(newIndexFiles, that.newIndexFiles)
                && Objects.equals(deletedIndexFiles, that.deletedIndexFiles);
    }

    @Override
    public int hashCode() {
        List<IndexFileMeta> all = new ArrayList<>(newIndexFiles);
        all.addAll(deletedIndexFiles);
        return Objects.hash(all);
    }

    @Override
    public String toString() {
        return "IndexIncrement{"
                + "newIndexFiles="
                + newIndexFiles
                + ",deletedIndexFiles="
                + deletedIndexFiles
                + "}";
    }
}
