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

import org.apache.flink.table.store.table.sink.FileCommittable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Manifest commit message. */
public class ManifestCommittable {

    private final long identifier;
    private final Map<Integer, Long> logOffsets;
    private final List<FileCommittable> fileCommittables;

    public ManifestCommittable(long identifier) {
        this.identifier = identifier;
        this.logOffsets = new HashMap<>();
        this.fileCommittables = new ArrayList<>();
    }

    public ManifestCommittable(
            long identifier,
            Map<Integer, Long> logOffsets,
            List<FileCommittable> fileCommittables) {
        this.identifier = identifier;
        this.logOffsets = logOffsets;
        this.fileCommittables = fileCommittables;
    }

    public void addFileCommittable(FileCommittable fileCommittable) {
        fileCommittables.add(fileCommittable);
    }

    public void addLogOffset(int bucket, long offset) {
        if (logOffsets.containsKey(bucket)) {
            throw new RuntimeException(
                    String.format(
                            "bucket-%d appears multiple times, which is not possible.", bucket));
        }
        logOffsets.put(bucket, offset);
    }

    public long identifier() {
        return identifier;
    }

    public Map<Integer, Long> logOffsets() {
        return logOffsets;
    }

    public List<FileCommittable> fileCommittables() {
        return fileCommittables;
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
                && Objects.equals(fileCommittables, that.fileCommittables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, logOffsets, fileCommittables);
    }

    @Override
    public String toString() {
        return String.format(
                "ManifestCommittable {"
                        + "identifier = %s, "
                        + "logOffsets = %s, "
                        + "fileCommittables = %s",
                identifier, logOffsets, fileCommittables);
    }
}
