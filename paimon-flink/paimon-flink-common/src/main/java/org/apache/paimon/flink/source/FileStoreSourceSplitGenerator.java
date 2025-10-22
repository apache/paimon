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

package org.apache.paimon.flink.source;

import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * The {@code FileStoreSplitGenerator}'s task is to plan all files to be read and to split them into
 * a set of {@link FileStoreSourceSplit}.
 */
public class FileStoreSourceSplitGenerator {

    private final String uuid = UUID.randomUUID().toString();
    private final AtomicInteger idCounter = new AtomicInteger(1);

    public List<FileStoreSourceSplit> createSplits(TableScan.Plan plan) {
        return createSplits(plan.splits());
    }

    public List<FileStoreSourceSplit> createSplits(List<Split> splits) {
        return splits.stream()
                .map(s -> new FileStoreSourceSplit(getNextId(), s))
                .collect(Collectors.toList());
    }

    protected final String getNextId() {
        return uuid + "-" + idCounter.getAndIncrement();
    }
}
