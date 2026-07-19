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
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/** Serializable catalog access for a managed format table. */
@Experimental
public class FormatTableCatalogProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int PARTITION_PAGE_SIZE = 1000;
    private static final int MAX_CACHED_PATTERNS = 128;
    private static final int MAX_TRACKED_TABLE_GENERATIONS = 10_000;
    private static final Duration PARTITION_CACHE_TTL = Duration.ofSeconds(30);
    private static final Duration TABLE_GENERATION_TTL = Duration.ofHours(1);

    // Keyed by identifier full name: listings themselves live in per-instance caches, so sharing
    // a generation counter across table incarnations (or across catalogs using the same name) can
    // only cause an extra invalidation, never a stale read.
    private static final Cache<String, AtomicLong> GENERATIONS =
            Caffeine.newBuilder()
                    .expireAfterAccess(TABLE_GENERATION_TTL)
                    .maximumSize(MAX_TRACKED_TABLE_GENERATIONS)
                    .executor(Runnable::run)
                    .build();

    private final Identifier identifier;
    private final CatalogLoader catalogLoader;

    @Nullable private transient Cache<String, List<Partition>> partitionCache;
    // Reused across list/create calls: constructing a Catalog (HTTP client, auth provider, and
    // for some auth providers an initial token fetch) per partition operation is expensive, and
    // RESTCatalog.close() is a no-op so a long-lived instance leaks nothing. Recreated lazily
    // after deserialization.
    @Nullable private transient Catalog catalog;

    public FormatTableCatalogProvider(Identifier identifier, CatalogLoader catalogLoader) {
        this.identifier = identifier;
        this.catalogLoader = catalogLoader;
    }

    /** List every catalog-visible partition matching the optional name prefix pattern. */
    public List<Partition> listPartitions(@Nullable String partitionNamePattern) {
        return cache().get(
                        partitionCacheKey(partitionNamePattern),
                        ignored -> loadPartitions(partitionNamePattern));
    }

    /**
     * Advance the partition-listing generation of every provider of this table in this JVM, so
     * same-process scans skip cached listings taken before the mutation. The catalog partition DDL
     * path (create/dropPartitions) calls this from a {@code finally} block: the mutation's outcome
     * can be ambiguous (the server may have committed even when the response was lost), so the
     * cached listing must be dropped after every attempt, not only after a confirmed success.
     */
    public static void advanceGeneration(Identifier identifier) {
        GENERATIONS.get(identifier.getFullName(), ignored -> new AtomicLong()).incrementAndGet();
    }

    /** Create partitions with the catalog's idempotent create contract. */
    public void createPartitions(List<Map<String, String>> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            // Register in bounded batches: one backfill commit can touch tens of thousands of
            // partitions (e.g. years of hourly data in a single dynamic-partition INSERT), and a
            // single unbounded request either monopolizes a catalog worker or trips server-side
            // request caps, failing the job after its data files are already committed.
            // Registration is an idempotent ADD-only upsert, so a mid-way failure leaves a state
            // that a rerun or MSCK converges from.
            for (int start = 0; start < partitions.size(); start += PARTITION_PAGE_SIZE) {
                catalog()
                        .createPartitions(
                                identifier,
                                partitions.subList(
                                        start,
                                        Math.min(start + PARTITION_PAGE_SIZE, partitions.size())));
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to register partitions for managed format table %s.",
                            identifier),
                    e);
        } finally {
            advanceGeneration(identifier);
            cache().invalidateAll();
        }
    }

    private List<Partition> loadPartitions(@Nullable String partitionNamePattern) {
        List<Partition> partitions = new ArrayList<>();
        Set<String> seenPageTokens = new HashSet<>();
        try {
            Catalog catalog = catalog();
            String pageToken = null;
            do {
                PagedList<Partition> page =
                        catalog.listPartitionsPaged(
                                identifier, PARTITION_PAGE_SIZE, pageToken, partitionNamePattern);
                partitions.addAll(page.getElements());
                pageToken = page.getNextPageToken();
                if (StringUtils.isNotEmpty(pageToken) && !seenPageTokens.add(pageToken)) {
                    throw new IllegalStateException(
                            String.format(
                                    "Catalog returned repeated partition page token '%s' for managed format table %s.",
                                    pageToken, identifier.getFullName()));
                }
            } while (StringUtils.isNotEmpty(pageToken));
            return Collections.unmodifiableList(partitions);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to list partitions for managed format table %s.", identifier),
                    e);
        }
    }

    private synchronized Catalog catalog() {
        if (catalog == null) {
            catalog = catalogLoader.load();
        }
        return catalog;
    }

    private synchronized Cache<String, List<Partition>> cache() {
        if (partitionCache == null) {
            partitionCache =
                    Caffeine.newBuilder()
                            .expireAfterWrite(PARTITION_CACHE_TTL)
                            .maximumSize(MAX_CACHED_PATTERNS)
                            .executor(Runnable::run)
                            .build();
        }
        return partitionCache;
    }

    private String partitionCacheKey(@Nullable String partitionNamePattern) {
        // Partition mutations advance a process-local generation so providers in this JVM skip
        // stale entries. Providers in other JVMs converge when PARTITION_CACHE_TTL expires.
        return generation().get() + "\000" + partitionNamePattern;
    }

    private AtomicLong generation() {
        return GENERATIONS.get(identifier.getFullName(), ignored -> new AtomicLong());
    }
}
