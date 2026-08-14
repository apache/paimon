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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.ManagedBlobOrphanFilesClean;
import org.apache.paimon.table.FileStoreTable;

import java.util.List;
import java.util.function.Consumer;

/** Java bridge for FileIO-aware managed blob candidate identities. */
abstract class SparkManagedBlobOrphanFilesCleanBase extends ManagedBlobOrphanFilesClean {

    SparkManagedBlobOrphanFilesCleanBase(
            FileStoreTable table, long olderThanMillis, boolean dryRun) {
        super(table, olderThanMillis, dryRun);
    }

    final String packIdentityForCandidate(Path path) {
        return packIdentityForCleanup(path).orElse(SKIP_MANAGED_BLOB_GC);
    }

    final List<SidecarWorkItem> createSidecarWorkItemsForSpark(
            ManifestEntry entry, DataFilePathFactory pathFactory) {
        return createSidecarWorkItems(entry, pathFactory);
    }

    final void emitUsedPacksForSpark(
            SidecarWorkItem workItem, ReachabilityScan scan, Consumer<String> used) {
        emitUsedPacks(workItem, scan, used);
    }
}
