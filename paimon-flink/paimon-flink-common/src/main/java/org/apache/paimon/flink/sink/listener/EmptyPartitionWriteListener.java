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

package org.apache.paimon.flink.sink.listener;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.flink.sink.CommitterOperator;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link CommitListener} that adds an empty data file when a static partition is overwritten by
 * an empty dataset.
 */
public class EmptyPartitionWriteListener implements CommitListener {

    private static final Logger LOG = LoggerFactory.getLogger(EmptyPartitionWriteListener.class);

    private final FileStoreTable table;
    private final BatchTableCommit tableCommit;
    private final Map<String, String> overwritePartitionSpec;

    public EmptyPartitionWriteListener(
            FileStoreTable table,
            BatchTableCommit tableCommit,
            Map<String, String> overwritePartitionSpec) {
        this.table = table;
        this.tableCommit = tableCommit;
        this.overwritePartitionSpec = overwritePartitionSpec;
    }

    public static Optional<EmptyPartitionWriteListener> create(
            FileStoreTable table,
            BatchTableCommit tableCommit,
            @Nullable Map<String, String> overwritePartitionSpec) {
        if (!table.coreOptions().writeEmptyPartitionEnable()
                || overwritePartitionSpec == null
                || overwritePartitionSpec.isEmpty()) {
            return Optional.empty();
        }
        LOG.info("Create empty write listener: {}", overwritePartitionSpec);
        return Optional.of(
                new EmptyPartitionWriteListener(table, tableCommit, overwritePartitionSpec));
    }

    @Override
    public void notifyCommittable(List<ManifestCommittable> committables) {
        LOG.info("Empty write, overwritePartitions: {}", overwritePartitionSpec);
        Preconditions.checkArgument(
                overwritePartitionSpec != null && !overwritePartitionSpec.isEmpty(),
                "Overwrite partitions must not be empty");
        if (!isStaticOverwrite(table, overwritePartitionSpec)
                || !CommitterOperator.isEndInputCommit(committables)
                || hasNewFiles(committables)) {
            return;
        }
        BinaryRow overwritePartition =
                convertSpecToBinaryRow(
                        overwritePartitionSpec, table.schema().logicalPartitionType());
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            ((InnerTableWrite) write).writeEmptyFile(overwritePartition, 0);
            List<CommitMessage> commitMessages = write.prepareCommit();
            this.tableCommit.commit(commitMessages);
            LOG.info("Commit for empty overwrite: {}.", overwritePartitionSpec);
        } catch (Exception e) {
            LOG.error("Failed to prepare commit for empty partition write.", e);
            throw new RuntimeException("Failed to prepare commit for empty partition write.", e);
        }
    }

    @Override
    public void snapshotState() throws Exception {}

    @Override
    public void close() throws IOException {}

    private static boolean hasNewFiles(List<ManifestCommittable> commitList) {
        if (commitList == null || commitList.isEmpty()) {
            return false;
        }
        for (ManifestCommittable committable : commitList) {
            List<CommitMessage> commitMessages = committable.fileCommittables();
            if (commitMessages == null || commitMessages.isEmpty()) {
                continue;
            }
            for (CommitMessage commitMessage : commitMessages) {
                DataIncrement dataIncrement =
                        ((CommitMessageImpl) commitMessage).newFilesIncrement();
                if (dataIncrement != null
                        && dataIncrement.newFiles() != null
                        && !dataIncrement.newFiles().isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isStaticOverwrite(
            FileStoreTable table, Map<String, String> overwritePartitionSpec) {
        return table.schema().partitionKeys().stream()
                .allMatch(overwritePartitionSpec::containsKey);
    }

    private BinaryRow convertSpecToBinaryRow(Map<String, String> spec, RowType partitionType) {
        return InternalSerializers.create(partitionType)
                .toBinaryRow(
                        InternalRowPartitionComputer.convertSpecToInternalRow(
                                spec, partitionType, table.coreOptions().partitionDefaultName()))
                .copy();
    }
}
