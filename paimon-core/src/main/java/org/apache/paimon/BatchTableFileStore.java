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

package org.apache.paimon;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.operation.BatchTableFileStoreWrite;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.BatchFileStoreTable;
import org.apache.paimon.types.RowType;

/** {@link FileStore} only used for {@link BatchFileStoreTable}. */
public class BatchTableFileStore extends AppendOnlyFileStore {

    public BatchTableFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            long schemaId,
            CoreOptions options,
            RowType partitionType,
            RowType bucketKeyType,
            RowType rowType) {
        super(fileIO, schemaManager, schemaId, options, partitionType, bucketKeyType, rowType);
    }

    @Override
    public BatchTableFileStoreWrite newWrite(
            String commitUser, ManifestCacheFilter manifestFilter) {
        return new BatchTableFileStoreWrite(
                fileIO,
                newRead(),
                schemaId,
                commitUser,
                getRowType(),
                pathFactory(),
                snapshotManager(),
                newScan(),
                options);
    }
}
