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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.utils.JsonSerdeUtil;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * Builds a portable {@link TableDescriptor} from a resolved {@link FileStoreTable}, and serializes
 * it to the cross-language JSON wire form.
 *
 * <p>A query engine that has already resolved a table for planning can ship this descriptor to a
 * non-Java reader (paimon-rust / paimon-cpp) so the reader rebuilds the table without re-resolving
 * it through a catalog. Only the reader-relevant metadata is captured — the table {@link
 * org.apache.paimon.table.DataTable#location() location} and its <b>persisted</b> schema; JVM-only
 * state, dynamic session options (including credentials), and the injected path option are
 * intentionally dropped. The resolved branch and, when time travel was requested, the resolved
 * snapshot id are carried explicitly.
 *
 * <p>Snapshot selection has two modes: if a reader consumes a co-shipped planner-provided split,
 * that split pins the exact files/snapshot; if a reader instead plans from this descriptor, it must
 * pin {@link TableDescriptor#snapshotId()} when present to reproduce the resolved snapshot.
 *
 * <p>v1 supports plain file-store tables, including a specific (non-main) branch. Composite /
 * delegating tables (fallback-branch, chain, privileged) are rejected: they delegate {@code
 * location()}/{@code schema()} to their primary and would be serialized as a plain table, silently
 * losing their merge / fallback semantics.
 */
@Experimental
public final class TableDescriptorSerializer {

    private TableDescriptorSerializer() {}

    /**
     * Build a descriptor without database/table names (identifier is synthesized by the reader).
     */
    public static TableDescriptor from(FileStoreTable table) {
        return from(table, null, null);
    }

    /**
     * Build a descriptor, optionally carrying the database and table names for better reader-side
     * identifiers and diagnostics.
     */
    public static TableDescriptor from(
            FileStoreTable table, @Nullable String database, @Nullable String name) {
        if (table instanceof DelegatedFileStoreTable) {
            throw new UnsupportedOperationException(
                    "TableDescriptor v1 only supports plain file-store tables, got: "
                            + table.getClass().getName());
        }
        if ((database == null) != (name == null)) {
            throw new IllegalArgumentException(
                    "TableDescriptor must set both database and name, or neither");
        }
        // Branch is derived from the resolved table options (the persisted schema does not record
        // it). Omit "main" to keep the descriptor slim (absent == main).
        String branch = CoreOptions.branch(table.schema().options());
        String descriptorBranch = Identifier.DEFAULT_MAIN_BRANCH.equals(branch) ? null : branch;
        // If the planner requested time travel, ship the RESOLVED snapshot id (not the raw
        // scan.* selectors): the persisted schema strips those dynamic options, and carrying the
        // resolved result keeps the contract engine-/version-neutral (the reader just pins this
        // snapshot). Ordinary scans resolve to empty -> null (reader reads latest / the split pins
        // files).
        Optional<Snapshot> travelSnapshot = TimeTravelUtil.tryTravelToSnapshot(table);
        Long snapshotId = travelSnapshot.map(Snapshot::id).orElse(null);
        // Serialize the PERSISTED schema (schema-N), not table.schema(): the latter has dynamic
        // options merged in by AbstractFileStoreTable.copyInternal, which can include credentials
        // (e.g. fs.s3a.secret.key) and the injected path option. Storage options travel separately
        // (the reader's FFI takes them out of band). schemaManager() is branch-scoped, so for a
        // branch table this reads the branch's own persisted schema.
        TableSchema schema = table.schemaManager().schema(table.schema().id());
        return new TableDescriptor(
                TableDescriptor.CURRENT_VERSION,
                table.location().toString(),
                schema,
                database,
                name,
                descriptorBranch,
                snapshotId);
    }

    /** Serialize a descriptor to its JSON wire form via Paimon's {@link JsonSerdeUtil}. */
    public static String toJson(TableDescriptor descriptor) {
        return JsonSerdeUtil.toJson(descriptor);
    }

    /** Convenience: {@link #from(FileStoreTable, String, String)} then {@link #toJson}. */
    public static String serialize(
            FileStoreTable table, @Nullable String database, @Nullable String name) {
        return toJson(from(table, database, name));
    }
}
