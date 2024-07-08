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

import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A partition expiration policy that compares the last update time of the partition to the current
 * time.
 */
public class PartitionUpdateTimeExpireStrategy extends PartitionExpireStrategy {

    public PartitionUpdateTimeExpireStrategy(RowType partitionType) {
        super(partitionType);
    }

    @Override
    public List<PartitionEntry> selectExpiredPartitions(
            FileStoreScan scan, LocalDateTime expirationTime) {
        return scan.readPartitionEntries().stream()
                .filter(
                        partitionEntry ->
                                expirationTime.isAfter(
                                        DateTimeUtils.toLocalDateTime(
                                                partitionEntry.lastFileCreationTime())))
                .collect(Collectors.toList());
    }
}
