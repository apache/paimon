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

package org.apache.paimon.partition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Strategy for partition expiration. */
public abstract class PartitionExpireStrategy {

    protected final List<String> partitionKeys;
    protected final String partitionDefaultName;
    private final RowDataToObjectArrayConverter toObjectArrayConverter;

    public PartitionExpireStrategy(RowType partitionType, String partitionDefaultName) {
        this.toObjectArrayConverter = new RowDataToObjectArrayConverter(partitionType);
        this.partitionKeys = partitionType.getFieldNames();
        this.partitionDefaultName = partitionDefaultName;
    }

    public Map<String, String> toPartitionString(Object[] array) {
        Map<String, String> map = new LinkedHashMap<>(partitionKeys.size());
        for (int i = 0; i < partitionKeys.size(); i++) {
            map.put(partitionKeys.get(i), array[i].toString());
        }
        return map;
    }

    public List<String> toPartitionValue(Object[] array) {
        List<String> list = new ArrayList<>(partitionKeys.size());
        for (int i = 0; i < partitionKeys.size(); i++) {
            if (array[i] != null) {
                list.add(array[i].toString());
            } else {
                list.add(partitionDefaultName);
            }
        }
        return list;
    }

    public Object[] convertPartition(BinaryRow partition) {
        return toObjectArrayConverter.convert(partition);
    }

    public abstract List<PartitionEntry> selectExpiredPartitions(
            FileStoreScan scan, LocalDateTime expirationTime);

    public static PartitionExpireStrategy createPartitionExpireStrategy(
            CoreOptions options,
            RowType partitionType,
            @Nullable CatalogLoader catalogLoader,
            @Nullable Identifier identifier) {
        Optional<PartitionExpireStrategyFactory> custom =
                PartitionExpireStrategyFactory.INSTANCE.get();
        if (custom.isPresent()) {
            try {
                return custom.get().create(catalogLoader, identifier, options, partitionType);
            } catch (UnsupportedOperationException ignored) {
            }
        }

        String strategy = options.partitionExpireStrategy();
        switch (strategy) {
            case "update-time":
                return new PartitionUpdateTimeExpireStrategy(options, partitionType);
            case "values-time":
                return new PartitionValuesTimeExpireStrategy(options, partitionType);
            default:
                throw new IllegalArgumentException("Unknown partitionExpireStrategy: " + strategy);
        }
    }
}
