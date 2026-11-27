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
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.RoaringNavigableMap64;

/** Batch scan for append tables. */
public class AppendBatchTableScan extends AbstractBatchTableScan {

    public AppendBatchTableScan(
            TableSchema schema,
            SchemaManager schemaManager,
            CoreOptions options,
            SnapshotReader snapshotReader,
            TableQueryAuth queryAuth) {
        super(schema, schemaManager, options, snapshotReader, queryAuth);
    }

    /** Uses the candidate row IDs for manifest group pruning during this plan. */
    public Plan planWithManifestGroupCandidates(RoaringNavigableMap64 candidates) {
        ManifestsReader manifestsReader = snapshotReader.manifestsReader();
        manifestsReader.withManifestGroupCandidates(candidates);
        try {
            return plan();
        } finally {
            manifestsReader.withManifestGroupCandidates(null);
        }
    }
}
