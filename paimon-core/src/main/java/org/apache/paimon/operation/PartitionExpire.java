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
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Expire partitions. */
public class PartitionExpire {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionExpire.class);

    private final List<String> partitionKeys;
    private final RowDataToObjectArrayConverter toObjectArrayConverter;
    private final Duration expirationTime;
    private final Duration checkInterval;
    private final PartitionTimeExtractor timeExtractor;
    private final FileStoreScan scan;
    private final FileStoreCommit commit;
    private final MetastoreClient metastoreClient;

    private LocalDateTime lastCheck;

    public PartitionExpire(
            RowType partitionType,
            Duration expirationTime,
            Duration checkInterval,
            String timePattern,
            String timeFormatter,
            FileStoreScan scan,
            FileStoreCommit commit,
            @Nullable MetastoreClient metastoreClient) {
        this.partitionKeys = partitionType.getFieldNames();
        this.toObjectArrayConverter = new RowDataToObjectArrayConverter(partitionType);
        this.expirationTime = expirationTime;
        this.checkInterval = checkInterval;
        this.timeExtractor = new PartitionTimeExtractor(timePattern, timeFormatter);
        this.scan = scan;
        this.commit = commit;
        this.metastoreClient = metastoreClient;
        this.lastCheck = LocalDateTime.now();
    }

    public PartitionExpire withLock(Lock lock) {
        this.commit.withLock(lock);
        return this;
    }

    public void expire(long commitIdentifier) {
        expire(LocalDateTime.now(), commitIdentifier);
    }

    @VisibleForTesting
    void setLastCheck(LocalDateTime time) {
        lastCheck = time;
    }

    @VisibleForTesting
    void expire(LocalDateTime now, long commitIdentifier) {
        if (checkInterval.isZero() || now.isAfter(lastCheck.plus(checkInterval))) {
            doExpire(now.minus(expirationTime), commitIdentifier);
            lastCheck = now;
        }
    }

    private void doExpire(LocalDateTime expireDateTime, long commitIdentifier) {
        List<Map<String, String>> expired = new ArrayList<>();
        for (BinaryRow partition : readPartitions(expireDateTime)) {
            Object[] array = toObjectArrayConverter.convert(partition);
            Map<String, String> partString = toPartitionString(array);
            expired.add(partString);
            LOG.info("Expire Partition: " + partition);
        }
        if (expired.size() > 0) {
            commit.dropPartitions(expired, commitIdentifier);
            if (metastoreClient != null) {
                deleteMetaStorePartitions(expired);
            }
        }
    }

    private void deleteMetaStorePartitions(List<Map<String, String>> partitions) {
        if (metastoreClient != null) {
            partitions.forEach(partition -> {
                try {
                    metastoreClient.deletePartition(new LinkedHashMap<>(partition));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private Map<String, String> toPartitionString(Object[] array) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            map.put(partitionKeys.get(i), array[i].toString());
        }
        return map;
    }

    private List<BinaryRow> readPartitions(LocalDateTime expireDateTime) {
        return scan.withPartitionFilter(new PartitionTimePredicate(expireDateTime))
                .listPartitions();
    }

    private class PartitionTimePredicate implements PartitionPredicate {

        private final LocalDateTime expireDateTime;

        private PartitionTimePredicate(LocalDateTime expireDateTime) {
            this.expireDateTime = expireDateTime;
        }

        @Override
        public boolean test(BinaryRow partition) {
            Object[] array = toObjectArrayConverter.convert(partition);
            LocalDateTime partTime = timeExtractor.extract(partitionKeys, Arrays.asList(array));
            return partTime != null && expireDateTime.isAfter(partTime);
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
