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

package org.apache.paimon.rest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.TableType;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.rest.requests.CreatePartitionsRequest;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.table.format.FormatTablePartitionPathResolver;
import org.apache.paimon.table.format.FormatTablePartitionRegistryValidator;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.CoreOptions.PATH;

/** Helpers for validating partition options in the mock REST catalog. */
final class RESTCatalogPartitionSupport {

    private RESTCatalogPartitionSupport() {}

    @Nullable
    static List<Map<String, String>> canonicalizeRequestedOptions(
            CreatePartitionsRequest request,
            CatalogContext catalogContext,
            boolean optionCreateSupported) {
        List<Map<String, String>> options = request.getPartitionOptions();
        if (options == null) {
            return null;
        }
        List<Map<String, String>> specs = request.getPartitionSpecs();
        if (specs == null || options.size() != specs.size()) {
            throw new IllegalArgumentException(
                    "partitionOptions must contain exactly one entry for every partition spec.");
        }
        Set<Map<String, String>> uniqueSpecs = new HashSet<>();
        boolean hasOptions = false;
        for (int i = 0; i < options.size(); i++) {
            if (specs.get(i) == null || !uniqueSpecs.add(specs.get(i))) {
                throw new IllegalArgumentException(
                        "partitionSpecs must not contain duplicates when partitionOptions is present.");
            }
            Map<String, String> partitionOptions = options.get(i);
            if (partitionOptions == null) {
                throw new IllegalArgumentException("partitionOptions must not contain null maps.");
            }
            if (partitionOptions.entrySet().stream()
                    .anyMatch(entry -> entry.getKey() == null || entry.getValue() == null)) {
                throw new IllegalArgumentException(
                        "partitionOptions must not contain null keys or values.");
            }
            hasOptions |= !partitionOptions.isEmpty();
        }
        if (!hasOptions) {
            return null;
        }
        if (!optionCreateSupported) {
            throw new UnsupportedOperationException(
                    "This REST provider does not support partition options.");
        }
        List<Map<String, String>> canonical = new ArrayList<>(options.size());
        for (int i = 0; i < options.size(); i++) {
            Map<String, String> partitionOptions = options.get(i);
            Map<String, String> copied = new HashMap<>(partitionOptions);
            String location = copied.get(PATH.key());
            if (location != null) {
                // What a partition may own is judged once the location is known not to be the
                // partition's own default directory, which validateFormatTablePartitionLocations
                // does after the returns have been taken out.
                copied.put(
                        PATH.key(),
                        FormatTablePartitionPathResolver.canonicalizeLocation(
                                        location, catalogContext)
                                .toString());
            }
            canonical.add(copied);
        }
        return canonical;
    }

    static Optional<Map<String, String>> conflictingLocation(
            List<Partition> stored,
            List<Map<String, String>> requestedSpecs,
            @Nullable List<Map<String, String>> requestedOptions) {
        if (requestedOptions == null) {
            return Optional.empty();
        }
        Map<Map<String, String>, Partition> storedBySpec = new HashMap<>();
        for (Partition partition : stored) {
            storedBySpec.put(partition.spec(), partition);
        }
        for (int i = 0; i < requestedSpecs.size(); i++) {
            Map<String, String> spec = requestedSpecs.get(i);
            Partition existing = storedBySpec.get(spec);
            String requestedLocation = requestedOptions.get(i).get(PATH.key());
            if (existing != null
                    && requestedLocation != null
                    && !Objects.equals(customLocation(existing), requestedLocation)) {
                return Optional.of(spec);
            }
        }
        return Optional.empty();
    }

    static ErrorResponse conflictingLocationError(Map<String, String> spec) {
        String partitionName = PartitionUtils.buildPartitionName(spec);
        return new ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_PARTITION,
                partitionName,
                String.format(
                        "Partition %s already exists at a different location.", partitionName),
                409);
    }

    static Partition newPartition(Map<String, String> spec, @Nullable Map<String, String> options) {
        return new Partition(
                new LinkedHashMap<>(spec),
                PartitionStatistics.UNKNOWN,
                PartitionStatistics.UNKNOWN,
                PartitionStatistics.UNKNOWN,
                PartitionStatistics.UNKNOWN,
                PartitionStatistics.UNKNOWN_TOTAL_BUCKETS,
                false,
                null,
                null,
                null,
                null,
                normalizeNewPartitionOptions(options));
    }

    static List<Partition> copyPartitions(List<Partition> partitions) {
        List<Partition> copied = new ArrayList<>(partitions.size());
        for (Partition partition : partitions) {
            copied.add(copyPartition(partition, copyOptions(partition.options())));
        }
        return copied;
    }

    private static void validateNoAdditiveStatisticsForCustomPartitions(
            List<Partition> stored,
            @Nullable List<PartitionStatistics> statistics,
            @Nullable Boolean replaceStatistics) {
        if (statistics == null || statistics.isEmpty() || Boolean.TRUE.equals(replaceStatistics)) {
            return;
        }
        Set<Map<String, String>> reportedSpecs = new HashSet<>();
        for (PartitionStatistics statistic : statistics) {
            reportedSpecs.add(statistic.spec());
        }
        for (Partition partition : stored) {
            if (customLocation(partition) != null && reportedSpecs.contains(partition.spec())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot add statistics to custom-location partition %s; "
                                        + "reset its path with replacement statistics first.",
                                PartitionUtils.buildPartitionName(partition.spec())));
            }
        }
    }

    /**
     * Specs whose requested location names their own default directory. Such a request asks for the
     * partition to live there again, so the location itself is dropped from the request: a
     * partition at its default directory carries no location of its own.
     */
    static Set<Map<String, String>> takeReturnsToDefault(
            CreatePartitionsRequest request,
            @Nullable List<Map<String, String>> requestedOptions,
            List<Partition> stored,
            TableMetadata metadata,
            String tableName,
            CatalogContext catalogContext) {
        if (!TableType.FORMAT_TABLE
                .toString()
                .equalsIgnoreCase(metadata.schema().options().get(CoreOptions.TYPE.key()))) {
            canonicalizeRemainingLocations(requestedOptions, catalogContext);
            return java.util.Collections.emptySet();
        }
        validateNoAdditiveStatisticsForCustomPartitions(
                stored, request.getPartitionStatistics(), request.replaceStatistics());
        Set<Map<String, String>> returning =
                defaultDirectoryRequests(
                        request.getPartitionSpecs(),
                        requestedOptions,
                        metadata,
                        tableName,
                        catalogContext);
        validateReturnsToDefault(
                returning, request.getPartitionStatistics(), request.replaceStatistics());
        canonicalizeRemainingLocations(requestedOptions, catalogContext);
        return returning;
    }

    /**
     * A location still named after the returns were taken out is one a partition wants to own, so
     * it is held to the rules for such a place and stored the way they canonicalize it.
     */
    private static void canonicalizeRemainingLocations(
            @Nullable List<Map<String, String>> requestedOptions, CatalogContext catalogContext) {
        if (requestedOptions == null) {
            return;
        }
        for (Map<String, String> options : requestedOptions) {
            String location = options.get(PATH.key());
            if (location != null) {
                options.put(
                        PATH.key(),
                        FormatTablePartitionPathResolver.canonicalizeCustomLocation(
                                        location, catalogContext)
                                .toString());
            }
        }
    }

    private static Set<Map<String, String>> defaultDirectoryRequests(
            List<Map<String, String>> requestedSpecs,
            @Nullable List<Map<String, String>> requestedOptions,
            TableMetadata metadata,
            String tableName,
            CatalogContext catalogContext) {
        if (requestedOptions == null
                || requestedOptions.stream()
                        .noneMatch(options -> options.get(PATH.key()) != null)) {
            return java.util.Collections.emptySet();
        }
        String tablePath = metadata.schema().options().get(PATH.key());
        if (StringUtils.isBlank(tablePath)) {
            throw new IllegalStateException(
                    String.format("Format Table %s has no authoritative path.", tableName));
        }
        List<String> partitionKeys = metadata.schema().partitionKeys();
        boolean onlyValueInPath =
                new CoreOptions(metadata.schema().options()).formatTablePartitionOnlyValueInPath();
        Set<Map<String, String>> returning = new HashSet<>();
        for (int i = 0; i < requestedOptions.size(); i++) {
            Map<String, String> options = requestedOptions.get(i);
            String requested = options.get(PATH.key());
            Map<String, String> spec = requestedSpecs.get(i);
            if (requested == null || spec == null || !spec.keySet().containsAll(partitionKeys)) {
                continue;
            }
            LinkedHashMap<String, String> orderedSpec = new LinkedHashMap<>();
            for (String partitionKey : partitionKeys) {
                orderedSpec.put(partitionKey, spec.get(partitionKey));
            }
            if (FormatTablePartitionPathResolver.isDefaultPartitionPath(
                    new Path(tablePath), orderedSpec, onlyValueInPath, requested, catalogContext)) {
                options.remove(PATH.key());
                returning.add(spec);
            }
        }
        return returning;
    }

    /** Returning a partition to its default directory replaces whatever it held there. */
    private static void validateReturnsToDefault(
            Set<Map<String, String>> returning,
            @Nullable List<PartitionStatistics> statistics,
            @Nullable Boolean replaceStatistics) {
        if (returning.isEmpty()) {
            return;
        }
        Set<Map<String, String>> reportedSpecs = new HashSet<>();
        if (statistics != null) {
            for (PartitionStatistics statistic : statistics) {
                reportedSpecs.add(statistic.spec());
            }
        }
        for (Map<String, String> spec : returning) {
            if (!Boolean.TRUE.equals(replaceStatistics) || !reportedSpecs.contains(spec)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Returning partition %s to its default directory requires "
                                        + "replaceStatistics=true and statistics for the same partition.",
                                PartitionUtils.buildPartitionName(spec)));
            }
        }
    }

    /** Drops the location of every partition back at its default directory, then checks them. */
    static void settlePartitionLocations(
            List<Partition> partitions,
            Set<Map<String, String>> returningToDefault,
            boolean formatTable,
            TableMetadata metadata,
            String tableName,
            CatalogContext catalogContext) {
        applyReturnsToDefault(partitions, returningToDefault);
        if (formatTable) {
            validateFormatTablePartitionLocations(partitions, metadata, tableName, catalogContext);
        }
    }

    private static void applyReturnsToDefault(
            List<Partition> partitions, Set<Map<String, String>> returning) {
        if (returning.isEmpty()) {
            return;
        }
        for (int i = 0; i < partitions.size(); i++) {
            Partition partition = partitions.get(i);
            if (returning.contains(partition.spec())) {
                partitions.set(i, copyPartition(partition, withoutPath(partition.options())));
            }
        }
    }

    @Nullable
    private static Map<String, String> normalizeNewPartitionOptions(
            @Nullable Map<String, String> options) {
        Map<String, String> copied = copyOptions(options);
        if (copied == null) {
            return null;
        }
        return copied.isEmpty() ? null : copied;
    }

    @Nullable
    private static Map<String, String> withoutPath(@Nullable Map<String, String> options) {
        Map<String, String> copied = copyOptions(options);
        if (copied == null) {
            return null;
        }
        copied.remove(PATH.key());
        return copied.isEmpty() ? null : copied;
    }

    @Nullable
    private static Map<String, String> copyOptions(@Nullable Map<String, String> options) {
        return options == null ? null : new HashMap<>(options);
    }

    private static Partition copyPartition(
            Partition partition, @Nullable Map<String, String> options) {
        return new Partition(
                new LinkedHashMap<>(partition.spec()),
                partition.recordCount(),
                partition.fileSizeInBytes(),
                partition.fileCount(),
                partition.lastFileCreationTime(),
                partition.totalBuckets(),
                partition.done(),
                partition.createdAt(),
                partition.createdBy(),
                partition.updatedAt(),
                partition.updatedBy(),
                options);
    }

    private static void validateFormatTablePartitionLocations(
            List<Partition> partitions,
            TableMetadata metadata,
            String tableName,
            CatalogContext catalogContext) {
        if (partitions.stream().noneMatch(partition -> customLocation(partition) != null)) {
            return;
        }
        String tablePath = metadata.schema().options().get(PATH.key());
        if (StringUtils.isBlank(tablePath)) {
            throw new IllegalStateException(
                    String.format("Format Table %s has no authoritative path.", tableName));
        }
        try {
            FormatTablePartitionRegistryValidator.validatePartitionLocations(
                    partitions,
                    metadata.schema().partitionKeys(),
                    new Path(tablePath),
                    tableName,
                    new CoreOptions(metadata.schema().options())
                            .formatTablePartitionOnlyValueInPath(),
                    catalogContext);
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Nullable
    private static String customLocation(Partition partition) {
        return partition.options() == null ? null : partition.options().get(PATH.key());
    }
}
