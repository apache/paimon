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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.partition.PartitionValuesTimeExpireStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Expire partitions. */
public class PartitionExpire {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionExpire.class);

    private static final String DELIMITER = ",";

    private final Duration expirationTime;
    private final Duration checkInterval;
    private final FileStoreScan scan;
    private final FileStoreCommit commit;
    private final MetastoreClient metastoreClient;
    private LocalDateTime lastCheck;
    private final PartitionExpireStrategy strategy;
    private final boolean endInputCheckPartitionExpire;
    private int maxExpireNum;

    public PartitionExpire(
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy strategy,
            FileStoreScan scan,
            FileStoreCommit commit,
            @Nullable MetastoreClient metastoreClient,
            boolean endInputCheckPartitionExpire,
            int maxExpireNum) {
        this.expirationTime = expirationTime;
        this.checkInterval = checkInterval;
        this.strategy = strategy;
        this.scan = scan;
        this.commit = commit;
        this.metastoreClient = metastoreClient;
        this.lastCheck = LocalDateTime.now();
        this.endInputCheckPartitionExpire = endInputCheckPartitionExpire;
        this.maxExpireNum = maxExpireNum;
    }

    public PartitionExpire(
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy strategy,
            FileStoreScan scan,
            FileStoreCommit commit,
            @Nullable MetastoreClient metastoreClient,
            int maxExpireNum) {
        this(
                expirationTime,
                checkInterval,
                strategy,
                scan,
                commit,
                metastoreClient,
                false,
                maxExpireNum);
    }

    public PartitionExpire withLock(Lock lock) {
        this.commit.withLock(lock);
        return this;
    }

    public PartitionExpire withMaxExpireNum(int maxExpireNum) {
        this.maxExpireNum = maxExpireNum;
        return this;
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
            if (metastoreClient != null) {
                deleteMetastorePartitions(expired);
            }
            commit.dropPartitions(expired, commitIdentifier);
        }
        return expired;
    }

    private void deleteMetastorePartitions(List<Map<String, String>> partitions) {
        if (metastoreClient != null) {
            partitions.forEach(
                    partition -> {
                        try {
                            metastoreClient.deletePartition(new LinkedHashMap<>(partition));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    private List<Map<String, String>> convertToPartitionString(
            List<List<String>> expiredPartValues) {
        return expiredPartValues.stream()
                .map(values -> String.join(DELIMITER, values))
                .sorted()
                .map(s -> s.split(DELIMITER))
                .map(strategy::toPartitionString)
                .limit(Math.min(expiredPartValues.size(), maxExpireNum))
                .collect(Collectors.toList());
    }
}
