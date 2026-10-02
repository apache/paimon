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

package org.apache.paimon.flink.source.assigners;

import org.apache.paimon.flink.source.DynamicPartitionFilteringInfo;
import org.apache.paimon.flink.source.FileStoreSourceSplit;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Assigner to perform dynamic partition pruning by given {@link DynamicFilteringData}. */
public class DynamicPartitionPruningAssigner implements SplitAssigner {

    private final SplitAssigner innerAssigner;
    private final DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo;
    private final DynamicFilteringData dynamicFilteringData;

    public DynamicPartitionPruningAssigner(
            SplitAssigner innerAssigner,
            DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo,
            DynamicFilteringData dynamicFilteringData) {
        this.innerAssigner = innerAssigner;
        this.dynamicPartitionFilteringInfo = dynamicPartitionFilteringInfo;
        this.dynamicFilteringData = dynamicFilteringData;
    }

    @Override
    public List<FileStoreSourceSplit> getNext(int subtask, @Nullable String hostname) {
        List<FileStoreSourceSplit> sourceSplits = innerAssigner.getNext(subtask, hostname);
        while (!sourceSplits.isEmpty()) {
            List<FileStoreSourceSplit> filtered =
                    sourceSplits.stream().filter(this::filter).collect(Collectors.toList());
            if (!filtered.isEmpty()) {
                return filtered;
            }
            sourceSplits = innerAssigner.getNext(subtask, hostname);
        }

        return Collections.emptyList();
    }

    @Override
    public void addSplit(int suggestedTask, FileStoreSourceSplit splits) {
        if (filter(splits)) {
            innerAssigner.addSplit(suggestedTask, splits);
        }
    }

    @Override
    public void addSplitsBack(int subtask, List<FileStoreSourceSplit> splits) {
        innerAssigner.addSplitsBack(subtask, splits);
    }

    @Override
    public Collection<FileStoreSourceSplit> remainingSplits() {
        return innerAssigner.remainingSplits().stream()
                .filter(this::filter)
                .collect(Collectors.toList());
    }

    public static SplitAssigner createDynamicPartitionPruningAssignerIfNeeded(
            int subtaskId,
            SplitAssigner oriAssigner,
            DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo,
            SourceEvent sourceEvent,
            Logger logger) {
        DynamicFilteringData dynamicFilteringData = ((DynamicFilteringEvent) sourceEvent).getData();
        logger.info(
                "Received DynamicFilteringEvent: {}, is filtering: {}.",
                subtaskId,
                dynamicFilteringData.isFiltering());
        return dynamicFilteringData.isFiltering()
                ? new DynamicPartitionPruningAssigner(
                        oriAssigner, dynamicPartitionFilteringInfo, dynamicFilteringData)
                : oriAssigner;
    }

    @Override
    public Optional<Long> getNextSnapshotId(int subtask) {
        return innerAssigner.getNextSnapshotId(subtask);
    }

    @Override
    public int numberOfRemainingSplits() {
        return innerAssigner.numberOfRemainingSplits();
    }

    private boolean filter(FileStoreSourceSplit sourceSplit) {
        return dynamicPartitionFilteringInfo.mayMatch(dynamicFilteringData, sourceSplit.split());
    }
}
