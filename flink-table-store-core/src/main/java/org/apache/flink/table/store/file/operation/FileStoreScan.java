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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.predicate.Predicate;

import javax.annotation.Nullable;

import java.util.List;

/** Scan operation which produces a plan. */
public interface FileStoreScan {

    FileStoreScan withPartitionFilter(Predicate predicate);

    FileStoreScan withKeyFilter(Predicate predicate);

    FileStoreScan withValueFilter(Predicate predicate);

    FileStoreScan withBucket(int bucket);

    FileStoreScan withSnapshot(long snapshotId);

    FileStoreScan withManifestList(List<ManifestFileMeta> manifests);

    /** Produce a {@link Plan}. */
    Plan plan();

    /** Result plan of this scan. */
    interface Plan {

        /** Snapshot id of this plan, return null if manifest list is specified. */
        @Nullable
        Long snapshotId();

        /** Result {@link ManifestEntry} files. */
        List<ManifestEntry> files();
    }
}
