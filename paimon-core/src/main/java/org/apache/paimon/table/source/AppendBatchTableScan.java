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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

/** Batch scan for append tables. */
public class AppendBatchTableScan extends AbstractBatchTableScan {

    public static DataTableScan create(FileStoreTable table, SnapshotReader snapshotReader) {
        CoreOptions options = table.coreOptions();
        AppendBatchTableScan scan =
                new AppendBatchTableScan(
                        table.schema(),
                        table.schemaManager(),
                        options,
                        snapshotReader,
                        table.catalogEnvironment().tableQueryAuth(options));
        if (options.dataEvolutionEnabled()) {
            return new DataEvolutionBatchScan(table, scan);
        }
        return scan;
    }

    public AppendBatchTableScan(
            TableSchema schema,
            SchemaManager schemaManager,
            CoreOptions options,
            SnapshotReader snapshotReader,
            TableQueryAuth queryAuth) {
        super(schema, schemaManager, options, snapshotReader, queryAuth);
    }
}
