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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.PartitionEntry;
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

    public PartitionUpdateTimeExpireStrategy(CoreOptions options, RowType partitionType) {
        super(options, partitionType);
    }

    @Override
    public PartitionPredicate createPartitionPredicate(LocalDateTime expirationTime) {
        return new PartitionUpdateTimePredicate(expirationTime);
    }

    @Override
    public List<PartitionEntry> filterPartitionEntry(
            List<PartitionEntry> partitionEntries, LocalDateTime expirationTime) {
        return partitionEntries.stream()
                .filter(
                        partitionEntry ->
                                expirationTime.isAfter(
                                        DateTimeUtils.toLocalDateTime(
                                                partitionEntry.lastFileCreationTime())))
                .collect(Collectors.toList());
    }

    /** Use the partition's last update time to compare with the current time. */
    private class PartitionUpdateTimePredicate implements PartitionPredicate {

        private PartitionUpdateTimePredicate(LocalDateTime ignore) {}

        // Always expired, we need to aggregate all partition files creation time and then filter
        // it.
        @Override
        public boolean test(BinaryRow part) {
            return true;
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            return true;
        }
    }
}
