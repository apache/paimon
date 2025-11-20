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
import org.apache.paimon.table.PartitionHandler;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/** Expire partitions. */
public class PartitionExpire {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionExpire.class);

    private static final String DELIMITER = ",";

    private final Duration expirationTime;
    private final Duration checkInterval;
    private final FileStoreScan scan;
    private final FileStoreCommit commit;
    @Nullable private final PartitionHandler partitionHandler;
    private LocalDateTime lastCheck;
    private final PartitionExpireStrategy strategy;
    private final boolean endInputCheckPartitionExpire;
    private final int maxExpireNum;
    private final int expireBatchSize;

    public PartitionExpire(
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy strategy,
            FileStoreScan scan,
            FileStoreCommit commit,
            @Nullable PartitionHandler partitionHandler,
            boolean endInputCheckPartitionExpire,
            int maxExpireNum,
            int expireBatchSize) {
        this.expirationTime = expirationTime;
        this.checkInterval = checkInterval;
        this.strategy = strategy;
        this.scan = scan;
        this.commit = commit;
        this.partitionHandler = partitionHandler;
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

    public PartitionExpire(
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy strategy,
            FileStoreScan scan,
            FileStoreCommit commit,
            @Nullable PartitionHandler partitionHandler,
            int maxExpireNum,
            int expireBatchSize) {
        this(
                expirationTime,
                checkInterval,
                strategy,
                scan,
                commit,
                partitionHandler,
                false,
                maxExpireNum,
                expireBatchSize);
    }

    public List<Map<String, String>> expire(long commitIdentifier) {
        return expire(LocalDateTime.now(), commitIdentifier);
    }

    public boolean isValueExpiration() {
        return strategy instanceof PartitionValuesTimeExpireStrategy;
    }

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
        List<List<String>> expiredPartValues = new ArrayList<>(partitionEntries.size());
        for (PartitionEntry partition : partitionEntries) {
            Object[] array = strategy.convertPartition(partition.partition());
            expiredPartValues.add(strategy.toPartitionValue(array));
        }

        List<Map<String, String>> expired = new ArrayList<>();
        if (!expiredPartValues.isEmpty()) {
            // convert partition value to partition string, and limit the partition num
            expired = convertToPartitionString(expiredPartValues);
            LOG.info("Expire Partitions: {}", expired);
            if (expireBatchSize > 0 && expireBatchSize < expired.size()) {
                Lists.partition(expired, expireBatchSize)
                        .forEach(
                                expiredBatchPartitions ->
                                        doBatchExpire(expiredBatchPartitions, commitIdentifier));
            } else {
                doBatchExpire(expired, commitIdentifier);
            }
        }
        return expired;
    }

    private void doBatchExpire(
            List<Map<String, String>> expiredBatchPartitions, long commitIdentifier) {
        if (partitionHandler != null) {
            try {
                partitionHandler.dropPartitions(expiredBatchPartitions);
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException(e);
            }
        } else {
            commit.dropPartitions(expiredBatchPartitions, commitIdentifier);
        }
    }

    private List<Map<String, String>> convertToPartitionString(
            List<List<String>> expiredPartValues) {
        return expiredPartValues.stream()
                .map(values -> String.join(DELIMITER, values))
                .sorted()
                // Use split(DELIMITER, -1) to preserve trailing empty strings
                .map(s -> s.split(DELIMITER, -1))
                .map(strategy::toPartitionString)
                .limit(Math.min(expiredPartValues.size(), maxExpireNum))
                .collect(Collectors.toList());
    }
}
