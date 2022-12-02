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

package org.apache.flink.table.store.table.metadata;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.table.Table;

import static org.apache.flink.table.store.table.metadata.OptionsTable.OPTIONS;
import static org.apache.flink.table.store.table.metadata.SchemasTable.SCHEMAS;
import static org.apache.flink.table.store.table.metadata.SnapshotsTable.SNAPSHOTS;

/** Loader to load metadata {@link Table}s. */
public class MetadataTableLoader {

    public static Table load(String metadata, Path location) {
        switch (metadata.toLowerCase()) {
            case SNAPSHOTS:
                return new SnapshotsTable(location);
            case OPTIONS:
                return new OptionsTable(location);
            case SCHEMAS:
                return new SchemasTable(location);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported metadata table type: " + metadata);
        }
    }
}
