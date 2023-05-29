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

    private Map<Identifier, ManifestCommittable> manifestCommittables;

    public WrappedManifestCommittable() {
        this.manifestCommittables =
                new TreeMap<>(
                        Comparator.comparing(Identifier::getDatabaseName)
                                .thenComparing(Identifier::getObjectName));
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

        if (manifestCommittables.size() != that.manifestCommittables.size()) {
            return false;
        }

        for (Map.Entry<Identifier, ManifestCommittable> entry : manifestCommittables.entrySet()) {
            if (!Objects.equals(entry.getValue(), that.manifestCommittables.get(entry.getKey()))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(manifestCommittables.values().toArray(new Object[0]));
    }

    @Override
    public String toString() {
        return String.format(
                "WrappedManifestCommittable {" + "manifestCommittables = %s",
                formatManifestCommittables());
    }

    private String formatManifestCommittables() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Identifier id : manifestCommittables.keySet()) {
            ManifestCommittable committable = manifestCommittables.get(id);
            sb.append(String.format("%s=%s, ", id.getFullName(), committable.toString()));
        }
        if (manifestCommittables.size() > 0) {
            sb.delete(sb.length() - 2, sb.length());
        }
        sb.append("}");
        return sb.toString();
    }

    public ManifestCommittable computeCommittableIfAbsent(
            Identifier identifier, long checkpointId, long watermark) {
        return manifestCommittables.computeIfAbsent(
                identifier, id -> new ManifestCommittable(checkpointId, watermark));
    }

    public ManifestCommittable putManifestCommittable(
            Identifier identifier, ManifestCommittable manifestCommittable) {
        return manifestCommittables.put(identifier, manifestCommittable);
    }

    public Map<Identifier, ManifestCommittable> getManifestCommittables() {
        return manifestCommittables;
    }
}
