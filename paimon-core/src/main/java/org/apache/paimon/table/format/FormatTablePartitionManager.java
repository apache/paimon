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

package org.apache.paimon.table.format;

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.predicate.Predicate;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * The catalog partition registrations of a single Format Table with catalog-managed partitions.
 *
 * <p>This manages registration metadata only: it never creates, deletes or resolves a partition
 * directory. Partition values are raw values, never escaped directory names, and building
 * directories from them — including keeping them inside the table — is the caller's job.
 *
 * <p>Paging, page tokens, name patterns and per-request batching are implementation details; every
 * method here takes or returns its complete argument.
 *
 * @since 1.5
 */
@Experimental
public interface FormatTablePartitionManager extends Serializable {

    /**
     * List every partition whose leading partition values match {@code prefix}, or all partitions
     * when it is empty. The prefix must be a contiguous leading subset of the table's partition
     * keys.
     *
     * <p>The optional {@code filter} is a pushdown hint combined with the prefix as a conjunction:
     * an implementation may apply it partially or not at all, so the result is a superset of the
     * matching partitions and never misses one. Callers keep applying their full predicate on the
     * returned partitions.
     */
    List<Partition> listPartitions(Map<String, String> prefix, @Nullable Predicate filter);

    /** Return those of the given complete partition specs that are registered. */
    List<Partition> listPartitionsByNames(List<Map<String, String>> partitions);

    /**
     * Register partitions. With {@code ignoreIfExists=false} the whole batch is rejected when any
     * partition already exists, so such a request is never split.
     */
    void createPartitions(List<Map<String, String>> partitions, boolean ignoreIfExists);

    /** Unregister partitions. Metadata only; missing partitions are ignored. */
    void dropPartitions(List<Map<String, String>> partitions);

    /** Create a manager reading and writing partitions through a {@link CatalogLoader}. */
    static FormatTablePartitionManager create(
            Identifier identifier, List<String> partitionKeys, CatalogLoader catalogLoader) {
        return new CatalogFormatTablePartitionManager(identifier, partitionKeys, catalogLoader);
    }
}
