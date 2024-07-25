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
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A partition expiration policy that compare the time extracted from the partition with the current
 * time.
 */
public class PartitionValuesTimeExpireStrategy extends PartitionExpireStrategy {

    private static final Logger LOG =
            LoggerFactory.getLogger(PartitionValuesTimeExpireStrategy.class);

    private final PartitionTimeExtractor timeExtractor;

    public PartitionValuesTimeExpireStrategy(CoreOptions options, RowType partitionType) {
        super(partitionType);
        String timePattern = options.partitionTimestampPattern();
        String timeFormatter = options.partitionTimestampFormatter();
        this.timeExtractor = new PartitionTimeExtractor(timePattern, timeFormatter);
    }

    @Override
    public List<PartitionEntry> selectExpiredPartitions(
            FileStoreScan scan, LocalDateTime expirationTime) {
        return scan.withPartitionFilter(new PartitionValuesTimePredicate(expirationTime))
                .readPartitionEntries();
    }

    /** The expired partition predicate uses the date-format value of the partition. */
    private class PartitionValuesTimePredicate implements PartitionPredicate {

        private final LocalDateTime expireDateTime;

        private PartitionValuesTimePredicate(LocalDateTime expireDateTime) {
            this.expireDateTime = expireDateTime;
        }

        @Override
        public boolean test(BinaryRow partition) {
            Object[] array = convertPartition(partition);
            try {
                LocalDateTime partTime = timeExtractor.extract(partitionKeys, Arrays.asList(array));
                return expireDateTime.isAfter(partTime);
            } catch (DateTimeParseException e) {
                String partitionInfo =
                        IntStream.range(0, partitionKeys.size())
                                .mapToObj(i -> partitionKeys.get(i) + ":" + array[i])
                                .collect(Collectors.joining(","));
                LOG.warn(
                        "Can't extract datetime from partition {}. If you want to configure partition expiration, please:\n"
                                + "  1. Check the expiration configuration.\n"
                                + "  2. Manually delete the partition using the drop-partition command if the partition"
                                + " value is non-date formatted.\n"
                                + "  3. Use '{}' expiration strategy by set '{}', which supports non-date formatted partition.",
                        partitionInfo,
                        CoreOptions.PartitionExpireStrategy.UPDATE_TIME,
                        CoreOptions.PARTITION_EXPIRATION_STRATEGY.key());
                return false;
            }
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
