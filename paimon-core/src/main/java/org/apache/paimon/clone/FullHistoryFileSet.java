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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Files reachable from the full history of a Paimon table. */
public class FullHistoryFileSet {

    private final Set<Path> metadataFiles;
    private final Set<Path> dataFiles;
    private final Set<Path> indexFiles;
    private final Set<PayloadPath> dataPayloadPaths;
    private final Set<PayloadPath> indexPayloadPaths;

    private FullHistoryFileSet(
            Set<Path> metadataFiles,
            Set<PayloadPath> dataPayloadPaths,
            Set<PayloadPath> indexPayloadPaths) {
        this.metadataFiles = Collections.unmodifiableSet(new LinkedHashSet<>(metadataFiles));
        this.dataPayloadPaths = Collections.unmodifiableSet(new LinkedHashSet<>(dataPayloadPaths));
        this.indexPayloadPaths =
                Collections.unmodifiableSet(new LinkedHashSet<>(indexPayloadPaths));
        this.dataFiles = paths(dataPayloadPaths);
        this.indexFiles = paths(indexPayloadPaths);
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

    Set<PayloadPath> dataPayloadPaths() {
        return dataPayloadPaths;
    }

    Set<PayloadPath> indexPayloadPaths() {
        return indexPayloadPaths;
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

    private static Set<Path> paths(Set<PayloadPath> payloadPaths) {
        Set<Path> paths = new LinkedHashSet<>();
        for (PayloadPath payloadPath : payloadPaths) {
            paths.add(payloadPath.path());
        }
        return Collections.unmodifiableSet(paths);
    }

    static class PayloadPath {

        private final Path path;
        @Nullable private final Path mappingAnchor;

        private PayloadPath(Path path, @Nullable Path mappingAnchor) {
            this.path = path;
            this.mappingAnchor = mappingAnchor;
        }

        Path path() {
            return path;
        }

        @Nullable
        Path mappingAnchor() {
            return mappingAnchor;
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof PayloadPath)) {
                return false;
            }
            PayloadPath that = (PayloadPath) object;
            return path.equals(that.path) && Objects.equals(mappingAnchor, that.mappingAnchor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, mappingAnchor);
        }
    }

    static class Builder {

        private final Set<Path> metadataFiles = new LinkedHashSet<>();
        private final Set<PayloadPath> dataPayloadPaths = new LinkedHashSet<>();
        private final Set<PayloadPath> indexPayloadPaths = new LinkedHashSet<>();

        boolean addMetadataFile(Path path) {
            return metadataFiles.add(path);
        }

        void addDataFile(Path path) {
            addDataFile(path, null);
        }

        void addDataFile(Path path, @Nullable Path mappingAnchor) {
            dataPayloadPaths.add(new PayloadPath(path, mappingAnchor));
        }

        void addIndexFile(Path path) {
            addIndexFile(path, null);
        }

        void addIndexFile(Path path, @Nullable Path mappingAnchor) {
            indexPayloadPaths.add(new PayloadPath(path, mappingAnchor));
        }

        FullHistoryFileSet build() {
            return new FullHistoryFileSet(metadataFiles, dataPayloadPaths, indexPayloadPaths);
        }
    }
}
