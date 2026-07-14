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

package org.apache.paimon.clone;

import org.apache.paimon.fs.Path;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/** Files reachable from the full history of a Paimon table. */
public class FullHistoryFileSet {

    private final Set<Path> metadataFiles;
    private final Set<Path> dataFiles;
    private final Set<Path> indexFiles;

    private FullHistoryFileSet(Set<Path> metadataFiles, Set<Path> dataFiles, Set<Path> indexFiles) {
        this.metadataFiles = Collections.unmodifiableSet(new LinkedHashSet<>(metadataFiles));
        this.dataFiles = Collections.unmodifiableSet(new LinkedHashSet<>(dataFiles));
        this.indexFiles = Collections.unmodifiableSet(new LinkedHashSet<>(indexFiles));
    }

    public Set<Path> metadataFiles() {
        return metadataFiles;
    }

    public Set<Path> dataFiles() {
        return dataFiles;
    }

    public Set<Path> indexFiles() {
        return indexFiles;
    }

    public Set<Path> allFiles() {
        Set<Path> allFiles = new LinkedHashSet<>();
        allFiles.addAll(metadataFiles);
        allFiles.addAll(dataFiles);
        allFiles.addAll(indexFiles);
        return Collections.unmodifiableSet(allFiles);
    }

    public Set<Path> payloadFiles() {
        Set<Path> payloadFiles = new LinkedHashSet<>();
        payloadFiles.addAll(dataFiles);
        payloadFiles.addAll(indexFiles);
        return Collections.unmodifiableSet(payloadFiles);
    }

    static Builder builder() {
        return new Builder();
    }

    static class Builder {

        private final Set<Path> metadataFiles = new LinkedHashSet<>();
        private final Set<Path> dataFiles = new LinkedHashSet<>();
        private final Set<Path> indexFiles = new LinkedHashSet<>();

        boolean addMetadataFile(Path path) {
            return metadataFiles.add(path);
        }

        void addDataFile(Path path) {
            dataFiles.add(path);
        }

        void addIndexFile(Path path) {
            indexFiles.add(path);
        }

        FullHistoryFileSet build() {
            return new FullHistoryFileSet(metadataFiles, dataFiles, indexFiles);
        }
    }
}
