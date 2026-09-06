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

package org.apache.paimon.iceberg;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitPreCallback;

import java.util.List;

/**
 * Vetoes a commit whose schemas the Iceberg mirror cannot publish, before the snapshot becomes
 * visible. The mirror emits every schema from 0 to the latest, so a table that was already enabled
 * when an unsupported type entered its history keeps failing on the metadata it produces rather
 * than on the commit that produced it.
 *
 * <p>Memoized on the latest schema, so steady-state commits only read the latest schema file. The
 * comparison is by content: a rollback lets a later alteration reuse an id.
 */
public class IcebergPreCommitValidation implements CommitPreCallback {

    private final FileStoreTable table;

    private TableSchema validatedSchema;

    public IcebergPreCommitValidation(FileStoreTable table) {
        this.table = table;
    }

    @Override
    public void call(
            List<SimpleFileEntry> baseFiles,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot) {
        SchemaManager schemaManager = table.schemaManager();
        TableSchema latest = schemaManager.latest().get();
        if (latest.equals(validatedSchema)) {
            return;
        }
        SchemaValidation.validateHistoricalIcebergTypes(
                schemaManager::listAll, table.coreOptions());
        validatedSchema = latest;
    }

    @Override
    public void close() {}
}
