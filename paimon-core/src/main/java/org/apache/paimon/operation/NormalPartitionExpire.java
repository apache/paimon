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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.partition.PartitionValuesTimeExpireStrategy;
import org.apache.paimon.table.PartitionModification;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/** Expire partitions. */
public class NormalPartitionExpire implements PartitionExpire {

    private static final Logger LOG = LoggerFactory.getLogger(NormalPartitionExpire.class);

    private static final String DELIMITER = ",";

    private final Duration expirationTime;
    private final Duration checkInterval;
    private final FileStoreScan scan;
    private final FileStoreCommit commit;
    @Nullable private final PartitionModification partitionModification;
    private LocalDateTime lastCheck;
    private final PartitionExpireStrategy strategy;
    private final boolean endInputCheckPartitionExpire;
    private final int maxExpireNum;
    private final int expireBatchSize;

    public NormalPartitionExpire(
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy strategy,
            FileStoreScan scan,
            FileStoreCommit commit,
            @Nullable PartitionModification partitionModification,
            boolean endInputCheckPartitionExpire,
            int maxExpireNum,
            int expireBatchSize) {
        this.expirationTime = expirationTime;
        this.checkInterval = checkInterval;
        this.strategy = strategy;
        this.scan = scan;
        this.commit = commit;
        this.partitionModification = partitionModification;
        // Avoid the execution time of stream jobs from being too short and preventing partition
        // expiration
        long rndSeconds = 0;
        long checkIntervalSeconds = checkInterval.toMillis() / 1000;
        if (checkIntervalSeconds > 0) {
            rndSeconds = ThreadLocalRandom.current().nextLong(checkIntervalSeconds);
        }
        this.lastCheck = LocalDateTime.now().minusSeconds(rndSeconds);
        this.endInputCheckPartitionExpire = endInputCheckPartitionExpire;
        this.maxExpireNum = maxExpireNum;
        this.expireBatchSize = expireBatchSize;
    }

    public NormalPartitionExpire(
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy strategy,
            FileStoreScan scan,
            FileStoreCommit commit,
            @Nullable PartitionModification partitionModification,
            int maxExpireNum,
            int expireBatchSize) {
        this(
                expirationTime,
                checkInterval,
                strategy,
                scan,
                commit,
                partitionModification,
                false,
                maxExpireNum,
                expireBatchSize);
    }

    @Override
    public List<Map<String, String>> expire(long commitIdentifier) {
        return expire(LocalDateTime.now(), commitIdentifier);
    }

    @Override
    public boolean isValueExpiration() {
        return strategy instanceof PartitionValuesTimeExpireStrategy;
    }

    @Override
    public boolean isValueAllExpired(Collection<BinaryRow> partitions) {
        PartitionValuesTimeExpireStrategy valuesStrategy =
                (PartitionValuesTimeExpireStrategy) strategy;
        LocalDateTime expireDateTime = LocalDateTime.now().minus(expirationTime);
        for (BinaryRow partition : partitions) {
            if (!valuesStrategy.isExpired(expireDateTime, partition)) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    void setLastCheck(LocalDateTime time) {
        lastCheck = time;
    }

    @VisibleForTesting
    List<Map<String, String>> expire(LocalDateTime now, long commitIdentifier) {
        if (checkInterval.isZero()
                || now.isAfter(lastCheck.plus(checkInterval))
                || (endInputCheckPartitionExpire && Long.MAX_VALUE == commitIdentifier)) {
            List<Map<String, String>> expired =
                    doExpire(now.minus(expirationTime), commitIdentifier);
            lastCheck = now;
            return expired;
        }
        return null;
    }

    private List<Map<String, String>> doExpire(
            LocalDateTime expireDateTime, long commitIdentifier) {
        List<PartitionEntry> partitionEntries =
                strategy.selectExpiredPartitions(scan, expireDateTime);
        List<ExpiredPartition> expiredPartitions = new ArrayList<>(partitionEntries.size());
        for (PartitionEntry partition : partitionEntries) {
            Object[] array = strategy.convertPartition(partition.partition());
            // DATE and TIMESTAMP values are normalized for time extraction; report the stored
            // names instead.
            Map<String, String> partitionSpec =
                    commit.pathFactory()
                            .partitionComputer()
                            .generatePartValues(partition.partition());
            int index = 0;
            for (String value : partitionSpec.values()) {
                if (array[index] != null && strategy.isNormalizedForTimeExtraction(index)) {
                    array[index] = value;
                }
                index++;
            }
            List<String> values = strategy.toPartitionValue(array);
            // Sort on the joined form, but build the partition from the original values: a
            // value may itself contain DELIMITER, and splitting the joined string back apart
            // would then shift every following field by one.
            expiredPartitions.add(
                    new ExpiredPartition(
                            partition.partition(),
                            String.join(DELIMITER, values),
                            strategy.toPartitionString(values.toArray())));
        }

        List<ExpiredPartition> selected =
                expiredPartitions.stream()
                        .sorted(Comparator.comparing(p -> p.sortKey))
                        .limit(Math.min(expiredPartitions.size(), maxExpireNum))
                        .collect(Collectors.toList());
        List<Map<String, String>> expired =
                selected.stream().map(p -> p.name).collect(Collectors.toList());
        if (!selected.isEmpty()) {
            LOG.info("Expire Partitions: {}", expired);
            if (expireBatchSize > 0 && expireBatchSize < selected.size()) {
                Lists.partition(selected, expireBatchSize)
                        .forEach(
                                expiredBatchPartitions ->
                                        doBatchExpire(expiredBatchPartitions, commitIdentifier));
            } else {
                doBatchExpire(selected, commitIdentifier);
            }
        }
        return expired;
    }

    private void doBatchExpire(
            List<ExpiredPartition> expiredBatchPartitions, long commitIdentifier) {
        if (partitionModification != null) {
            // the metastore identifies partitions by their stored names
            List<Map<String, String>> names =
                    expiredBatchPartitions.stream().map(p -> p.name).collect(Collectors.toList());
            try {
                partitionModification.dropPartitions(names);
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException(e);
            }
        } else {
            // Drop the selected partitions themselves: a stored name is not always parsed back
            // to the same partition, e.g. when it equals the default partition name or is a
            // TIMESTAMP_LTZ value spelled in UTC but parsed in the local time zone.
            commit.dropBinaryPartitions(
                    expiredBatchPartitions.stream()
                            .map(p -> p.partition)
                            .collect(Collectors.toList()),
                    commitIdentifier);
        }
    }

    /**
     * A partition selected for expiration. The stored name is for reporting and the metastore,
     * while local deletion uses the original partition.
     */
    private static class ExpiredPartition {

        private final BinaryRow partition;
        private final String sortKey;
        private final Map<String, String> name;

        private ExpiredPartition(BinaryRow partition, String sortKey, Map<String, String> name) {
            this.partition = partition;
            this.sortKey = sortKey;
            this.name = name;
        }
    }
}
