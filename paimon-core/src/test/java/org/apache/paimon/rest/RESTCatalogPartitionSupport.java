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
                copied.put(
                        PATH.key(),
                        FormatTablePartitionPathResolver.canonicalizeCustomLocation(
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
                spec,
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
                options);
    }

    static void validateFormatTablePartitionLocations(
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
