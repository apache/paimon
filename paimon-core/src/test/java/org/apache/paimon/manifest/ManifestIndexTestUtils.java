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

import org.apache.paimon.FileStore;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Synthetic index references for manifest serialization and lifecycle tests. */
public final class ManifestIndexTestUtils {
    private ManifestIndexTestUtils() {}

    public static ManifestFileMeta withIndexFileName(ManifestFileMeta meta, String indexFileName) {
        return new ManifestFileMeta(
                meta.fileName(),
                meta.fileSize(),
                meta.numAddedFiles(),
                meta.numDeletedFiles(),
                meta.partitionStats(),
                meta.schemaId(),
                meta.minBucket(),
                meta.maxBucket(),
                meta.minLevel(),
                meta.maxLevel(),
                meta.minRowId(),
                meta.maxRowId(),
                indexFileName);
    }

    /** Replaces only synthetic snapshot fixtures, using newly written manifest lists. */
    public static void registerIndexReferences(FileStore<?> store, long snapshotId)
            throws IOException {
        SnapshotManager manager = store.snapshotManager();
        FileIO io = manager.fileIO();
        Path snapshotPath = manager.snapshotPath(snapshotId);
        ObjectNode snapshot =
                (ObjectNode)
                        JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(
                                io.readFileUtf8(snapshotPath));
        ManifestList lists = store.manifestListFactory().create();
        for (String field :
                new String[] {"baseManifestList", "deltaManifestList", "changelogManifestList"}) {
            JsonNode value = snapshot.get(field);
            if (value == null || value.isNull()) {
                continue;
            }
            List<ManifestFileMeta> indexed = new ArrayList<>();
            for (ManifestFileMeta meta : lists.read(value.asText())) {
                // Deliberately use a name which cannot be derived by appending the sidecar suffix.
                String name = "index-for-" + meta.fileName();
                Path index = store.pathFactory().toManifestFilePath(name);
                if (!io.exists(index)) {
                    // GC treats index bytes as opaque; unsupported/partial files are still owned.
                    io.newOutputStream(index, false).close();
                }
                indexed.add(withIndexFileName(meta, name));
            }
            Pair<String, Long> replacement = lists.write(indexed);
            snapshot.put(field, replacement.getLeft());
            snapshot.put(field + "Size", replacement.getRight());
        }
        io.overwriteFileUtf8(
                snapshotPath, JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.writeValueAsString(snapshot));
        manager.invalidateCache();
    }
}
