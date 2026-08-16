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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ObjectsCache;

import javax.annotation.Nullable;

/**
 * Filters for {@link ManifestEntry} for reading, it includes more partition filtering and bucket
 * filtering.
 */
public class ManifestEntryFilters extends ObjectsCache.Filters<ManifestEntry> {

    public final @Nullable PartitionPredicate partitionFilter;
    public final @Nullable BucketFilter bucketFilter;

    public ManifestEntryFilters(
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter,
            Filter<InternalRow> readFilter,
            Filter<ManifestEntry> readVFilter) {
        super(readFilter, readVFilter);
        this.partitionFilter = partitionFilter;
        this.bucketFilter = bucketFilter;
    }
}
