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

package org.apache.paimon.table.system;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.table.system.AllTableOptionsTable.ALL_TABLE_OPTIONS;
import static org.apache.paimon.table.system.AuditLogTable.AUDIT_LOG;
import static org.apache.paimon.table.system.ConsumersTable.CONSUMERS;
import static org.apache.paimon.table.system.FilesTable.FILES;
import static org.apache.paimon.table.system.ManifestsTable.MANIFESTS;
import static org.apache.paimon.table.system.OptionsTable.OPTIONS;
import static org.apache.paimon.table.system.PartitionsTable.PARTITIONS;
import static org.apache.paimon.table.system.SchemasTable.SCHEMAS;
import static org.apache.paimon.table.system.SnapshotsTable.SNAPSHOTS;
import static org.apache.paimon.table.system.TagsTable.TAGS;

/** Loader to load system {@link Table}s. */
public class SystemTableLoader {

    @Nullable
    public static Table load(String type, FileIO fileIO, FileStoreTable dataTable) {
        Path location = dataTable.location();
        switch (type.toLowerCase()) {
            case MANIFESTS:
                return new ManifestsTable(fileIO, location, dataTable);
            case SNAPSHOTS:
                return new SnapshotsTable(fileIO, location, dataTable);
            case OPTIONS:
                return new OptionsTable(fileIO, location);
            case SCHEMAS:
                return new SchemasTable(fileIO, location);
            case PARTITIONS:
                return new PartitionsTable(dataTable);
            case AUDIT_LOG:
                return new AuditLogTable(dataTable);
            case FILES:
                return new FilesTable(dataTable);
            case TAGS:
                return new TagsTable(fileIO, location);
            case CONSUMERS:
                return new ConsumersTable(fileIO, location);
            default:
                return null;
        }
    }

    @Nullable
    public static Table loadGlobal(
            String tableName, FileIO fileIO, Map<String, Map<String, Path>> allTablePaths) {
        switch (tableName.toLowerCase()) {
            case ALL_TABLE_OPTIONS:
                return new AllTableOptionsTable(fileIO, allTablePaths);
            default:
                return null;
        }
    }
}
