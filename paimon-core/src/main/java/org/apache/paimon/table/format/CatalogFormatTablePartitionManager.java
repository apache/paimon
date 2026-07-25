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

import org.apache.paimon.PagedList;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.utils.FunctionWithException;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link FormatTablePartitionManager} reading and writing partitions through a {@link Catalog}.
 * One catalog is created per operation and closed when it ends, so the table stays serializable and
 * no client outlives the call that needed it.
 */
class CatalogFormatTablePartitionManager implements FormatTablePartitionManager {

    private static final long serialVersionUID = 1L;

    /** Bounds one request: catalog services cap the partitions a single call may carry. */
    private static final int REQUEST_SIZE = 1000;

    private final Identifier identifier;
    private final List<String> partitionKeys;
    private final CatalogLoader catalogLoader;

    CatalogFormatTablePartitionManager(
            Identifier identifier, List<String> partitionKeys, CatalogLoader catalogLoader) {
        this.identifier = identifier;
        // Copied: the caller's list must not be able to change what counts as a leading prefix.
        this.partitionKeys = Collections.unmodifiableList(new ArrayList<>(partitionKeys));
        this.catalogLoader = catalogLoader;
    }

    @Override
    public List<Partition> listPartitions(Map<String, String> prefix, @Nullable Predicate filter) {
        LinkedHashMap<String, String> ordered = orderedPrefix(partitionKeys, prefix);
        checkArgument(
                ordered.size() == prefix.size(),
                "Partition prefix %s is not a leading prefix of partition keys %s of table %s.",
                prefix,
                partitionKeys,
                identifier.getFullName());
        String pattern = PartitionPathUtils.buildPartitionNamePrefixPattern(partitionKeys, ordered);
        return execute(
                catalog -> {
                    if (filter != null) {
                        return collectAllPages(
                                prefix,
                                pageToken ->
                                        catalog.listPartitionsByFilterPaged(
                                                identifier,
                                                filter,
                                                REQUEST_SIZE,
                                                pageToken,
                                                pattern));
                    }
                    return collectAllPages(
                            prefix,
                            pageToken ->
                                    catalog.listPartitionsPaged(
                                            identifier, REQUEST_SIZE, pageToken, pattern));
                },
                "list partitions");
    }

    /**
     * Drains a paged listing. Pages may be sparse (fewer elements than requested, or none, with a
     * next page token), so only an empty token ends the loop.
     */
    private List<Partition> collectAllPages(Map<String, String> prefix, PageSupplier pageSupplier)
            throws Exception {
        List<Partition> partitions = new ArrayList<>();
        Set<String> seenPageTokens = new HashSet<>();
        String pageToken = null;
        do {
            PagedList<Partition> page = pageSupplier.page(pageToken);
            if (page == null) {
                // A missing page cannot be told apart from an empty listing; failing loudly
                // beats silently reading fewer partitions.
                throw new IllegalStateException(
                        String.format(
                                "Catalog returned a null partition page for format table %s.",
                                identifier.getFullName()));
            }
            if (page.getElements() != null) {
                for (Partition partition : page.getElements()) {
                    if (matchesPrefix(partition, prefix)) {
                        partitions.add(partition);
                    }
                }
            }
            pageToken = page.getNextPageToken();
            if (StringUtils.isNotEmpty(pageToken) && !seenPageTokens.add(pageToken)) {
                throw new IllegalStateException(
                        String.format(
                                "Catalog returned repeated partition page token '%s' for format table %s.",
                                pageToken, identifier.getFullName()));
            }
        } while (StringUtils.isNotEmpty(pageToken));
        return Collections.unmodifiableList(partitions);
    }

    /** A single page of a partition listing. */
    private interface PageSupplier {
        PagedList<Partition> page(@Nullable String pageToken) throws Exception;
    }

    @Override
    public List<Partition> listPartitionsByNames(List<Map<String, String>> partitions) {
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }
        return execute(
                catalog -> {
                    List<Partition> found = new ArrayList<>();
                    for (List<Map<String, String>> batch : batches(partitions)) {
                        found.addAll(catalog.listPartitionsByNames(identifier, batch));
                    }
                    return Collections.unmodifiableList(found);
                },
                "list partitions by names");
    }

    @Override
    public void createPartitions(List<Map<String, String>> partitions, boolean ignoreIfExists) {
        if (partitions.isEmpty()) {
            return;
        }
        execute(
                catalog -> {
                    if (!ignoreIfExists) {
                        // Rejecting the whole batch when any partition exists is only meaningful
                        // if the batch stays one request, so a strict create is never split.
                        catalog.createPartitions(identifier, partitions, false);
                        return null;
                    }
                    // An idempotent create is safe to split: a rerun converges from a partially
                    // applied batch.
                    for (List<Map<String, String>> batch : batches(partitions)) {
                        catalog.createPartitions(identifier, batch, true);
                    }
                    return null;
                },
                "create partitions");
    }

    @Override
    public void dropPartitions(List<Map<String, String>> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        execute(
                catalog -> {
                    // Dropping ignores missing partitions, so a partially applied batch converges
                    // on a rerun and may be split.
                    for (List<Map<String, String>> batch : batches(partitions)) {
                        catalog.dropPartitions(identifier, batch);
                    }
                    return null;
                },
                "drop partitions");
    }

    private static boolean matchesPrefix(Partition partition, Map<String, String> prefix) {
        for (Map.Entry<String, String> entry : prefix.entrySet()) {
            if (!entry.getValue().equals(partition.spec().get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static List<List<Map<String, String>>> batches(List<Map<String, String>> partitions) {
        List<List<Map<String, String>>> batches = new ArrayList<>();
        for (int start = 0; start < partitions.size(); start += REQUEST_SIZE) {
            batches.add(
                    partitions.subList(start, Math.min(start + REQUEST_SIZE, partitions.size())));
        }
        return batches;
    }

    private <T> T execute(FunctionWithException<Catalog, T, Exception> operation, String what) {
        try (Catalog catalog = catalogLoader.load()) {
            return operation.apply(catalog);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to %s of format table %s.", what, identifier.getFullName()),
                    e);
        }
    }

    /** Order a prefix by the table partition keys, stopping at the first key it does not cover. */
    private static LinkedHashMap<String, String> orderedPrefix(
            List<String> partitionKeys, Map<String, String> prefix) {
        LinkedHashMap<String, String> ordered = new LinkedHashMap<>();
        for (String partitionKey : partitionKeys) {
            if (!prefix.containsKey(partitionKey)) {
                break;
            }
            ordered.put(partitionKey, prefix.get(partitionKey));
        }
        return ordered;
    }
}
