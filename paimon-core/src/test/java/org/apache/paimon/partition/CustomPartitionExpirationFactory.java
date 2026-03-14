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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Custom {@link PartitionExpireStrategyFactory}. */
public class CustomPartitionExpirationFactory implements PartitionExpireStrategyFactory {

    public static final Map<String, List<PartitionEntry>> TABLE_EXPIRE_PARTITIONS = new HashMap<>();

    @Override
    public PartitionExpireStrategy create(
            CatalogLoader catalogLoader,
            Identifier identifier,
            CoreOptions options,
            RowType partitionType) {
        String strategy = options.partitionExpireStrategy();
        if (!"custom".equals(strategy)) {
            throw new UnsupportedOperationException();
        }
        return new PartitionExpireStrategy(partitionType, options.partitionDefaultName()) {
            @Override
            public List<PartitionEntry> selectExpiredPartitions(
                    FileStoreScan scan, LocalDateTime expirationTime) {
                Table table;
                try (Catalog catalog = catalogLoader.load()) {
                    table = catalog.getTable(identifier);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return TABLE_EXPIRE_PARTITIONS.get(table.options().get("path"));
            }
        };
    }
}
