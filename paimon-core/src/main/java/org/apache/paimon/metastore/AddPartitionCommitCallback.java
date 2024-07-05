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

package org.apache.paimon.metastore;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;

import org.apache.paimon.shade.guava30.com.google.common.cache.Cache;
import org.apache.paimon.shade.guava30.com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.List;

/** A {@link CommitCallback} to add newly created partitions to metastore. */
public class AddPartitionCommitCallback implements CommitCallback {

    private final Cache<BinaryRow, Boolean> cache =
            CacheBuilder.newBuilder()
                    // avoid extreme situations
                    .expireAfterAccess(Duration.ofMinutes(30))
                    // estimated cache size
                    .maximumSize(300)
                    .softValues()
                    .build();

    private final MetastoreClient client;

    public AddPartitionCommitCallback(MetastoreClient client) {
        this.client = client;
    }

    @Override
    public void call(List<ManifestCommittable> committables) {
        committables.stream()
                .flatMap(c -> c.fileCommittables().stream())
                .map(CommitMessage::partition)
                .distinct()
                .forEach(this::addPartition);
    }

    private void addPartition(BinaryRow partition) {
        try {
            boolean added = cache.get(partition, () -> false);
            if (added) {
                return;
            }

            client.addPartition(partition);
            cache.put(partition, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
