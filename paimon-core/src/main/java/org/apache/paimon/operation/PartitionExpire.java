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
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.partition.PartitionExpireStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Expire partitions. */
public class PartitionExpire {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionExpire.class);

    private final Duration expirationTime;
    private final Duration checkInterval;
    private final FileStoreScan scan;
    private final FileStoreCommit commit;
    private final MetastoreClient metastoreClient;

    private LocalDateTime lastCheck;

    private final PartitionExpireStrategy strategy;

    public PartitionExpire(
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy strategy,
            FileStoreScan scan,
            FileStoreCommit commit,
            @Nullable MetastoreClient metastoreClient) {
        this.expirationTime = expirationTime;
        this.checkInterval = checkInterval;
        this.strategy = strategy;
        this.scan = scan;
        this.commit = commit;
        this.metastoreClient = metastoreClient;
        this.lastCheck = LocalDateTime.now();
    }

    public PartitionExpire withLock(Lock lock) {
        this.commit.withLock(lock);
        return this;
    }

    public List<Map<String, String>> expire(long commitIdentifier) {
        return expire(LocalDateTime.now(), commitIdentifier);
    }

    @VisibleForTesting
    void setLastCheck(LocalDateTime time) {
        lastCheck = time;
    }

    @VisibleForTesting
    List<Map<String, String>> expire(LocalDateTime now, long commitIdentifier) {
        if (checkInterval.isZero() || now.isAfter(lastCheck.plus(checkInterval))) {
            List<Map<String, String>> expired =
                    doExpire(now.minus(expirationTime), commitIdentifier);
            lastCheck = now;
            return expired;
        }
        return null;
    }

    private List<Map<String, String>> doExpire(
            LocalDateTime expireDateTime, long commitIdentifier) {
        List<Map<String, String>> expired = new ArrayList<>();
        for (PartitionEntry partition : strategy.selectExpiredPartitions(scan, expireDateTime)) {
            Object[] array = strategy.convertPartition(partition.partition());
            Map<String, String> partString = strategy.toPartitionString(array);
            expired.add(partString);
            LOG.info("Expire Partition: {}", partString);
        }
        if (!expired.isEmpty()) {
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
}
