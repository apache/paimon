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

package org.apache.paimon.manifest;

import org.apache.paimon.catalog.Identifier;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Manifest commit message. */
public class WrappedManifestCommittable {

    private final long checkpointId;

    private final long watermark;

    private final Map<Identifier, ManifestCommittable> manifestCommittables;

    public WrappedManifestCommittable(long checkpointId, long watermark) {
        this.checkpointId = checkpointId;
        this.watermark = watermark;
        this.manifestCommittables =
                new TreeMap<>(
                        Comparator.comparing(Identifier::getDatabaseName)
                                .thenComparing(Identifier::getObjectName));
    }

    public long checkpointId() {
        return checkpointId;
    }

    public long watermark() {
        return watermark;
    }

    public Map<Identifier, ManifestCommittable> manifestCommittables() {
        return manifestCommittables;
    }

    public ManifestCommittable computeCommittableIfAbsent(
            Identifier identifier, long checkpointId, long watermark) {
        return manifestCommittables.computeIfAbsent(
                identifier, id -> new ManifestCommittable(checkpointId, watermark));
    }

    public void putManifestCommittable(
            Identifier identifier, ManifestCommittable manifestCommittable) {
        manifestCommittables.put(identifier, manifestCommittable);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WrappedManifestCommittable that = (WrappedManifestCommittable) o;
        return checkpointId == that.checkpointId
                && watermark == that.watermark
                && Objects.equals(manifestCommittables, that.manifestCommittables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpointId, watermark, manifestCommittables);
    }

    @Override
    public String toString() {
        return "WrappedManifestCommittable{"
                + "checkpointId="
                + checkpointId
                + ", watermark="
                + watermark
                + ", manifestCommittables="
                + manifestCommittables
                + '}';
    }
}
