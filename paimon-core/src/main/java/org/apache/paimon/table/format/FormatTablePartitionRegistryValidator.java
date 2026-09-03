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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.Partition;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Rejects incomplete specs and partition locations that resolve to the same or nested paths. */
public final class FormatTablePartitionRegistryValidator {

    private FormatTablePartitionRegistryValidator() {}

    public static void validatePartitionLocations(
            List<Partition> partitions,
            List<String> partitionKeys,
            Path tablePath,
            String tableName,
            boolean onlyValueInPath,
            @Nullable CatalogContext catalogContext) {
        FormatTablePartitionPathResolver resolver =
                new FormatTablePartitionPathResolver(
                        tablePath, tableName, onlyValueInPath, catalogContext);
        for (Partition partition : partitions) {
            Map<String, String> spec = partition.spec();
            if (spec == null
                    || spec.size() != partitionKeys.size()
                    || !spec.keySet().containsAll(partitionKeys)) {
                throw new IllegalStateException(
                        String.format(
                                "Catalog returned incomplete partition spec %s for Format Table %s.",
                                spec, tableName));
            }
            LinkedHashMap<String, String> orderedSpec = new LinkedHashMap<>();
            for (String partitionKey : partitionKeys) {
                orderedSpec.put(partitionKey, spec.get(partitionKey));
            }
            Path resolved =
                    resolver.resolve(
                            orderedSpec,
                            FormatTablePartitionPathResolver.customLocation(partition));
            resolver.validateAndRecord(orderedSpec, resolved);
        }
    }
}
