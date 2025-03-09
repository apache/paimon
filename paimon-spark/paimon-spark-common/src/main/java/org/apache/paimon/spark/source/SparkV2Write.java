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

package org.apache.paimon.spark.source;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.spark.SparkInternalRowWrapper;
import org.apache.paimon.spark.SparkUtils;
import org.apache.paimon.spark.SparkWriteRequirement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.DYNAMIC_PARTITION_OVERWRITE;

/** Spark V2WRite. */
public class SparkV2Write implements Write, RequiresDistributionAndOrdering {
    private static final Logger LOG = LoggerFactory.getLogger(SparkV2Write.class);

    private final FileStoreTable table;
    private final BatchWriteBuilder writeBuilder;
    private final SparkWriteRequirement writeRequirement;
    private final boolean overwriteDynamic;
    private final Map<String, String> overwritePartitions;

    public SparkV2Write(
            FileStoreTable newTable,
            boolean overwriteDynamic,
            Map<String, String> overwritePartitions) {
        Preconditions.checkArgument(
                !overwriteDynamic || overwritePartitions == null,
                "Cannot overwrite dynamically and by filter both");

        this.table =
                newTable.copy(
                        ImmutableMap.of(
                                DYNAMIC_PARTITION_OVERWRITE.key(),
                                Boolean.toString(overwriteDynamic)));
        this.writeBuilder = table.newBatchWriteBuilder();
        if (overwritePartitions != null) {
            writeBuilder.withOverwrite(overwritePartitions);
        }

        this.writeRequirement = SparkWriteRequirement.of(table);
        this.overwriteDynamic = overwriteDynamic;
        this.overwritePartitions = overwritePartitions;
    }

    @Override
    public Distribution requiredDistribution() {
        Distribution distribution = writeRequirement.distribution();
        LOG.info("Requesting {} as write distribution for table {}", distribution, table.name());
        return distribution;
    }

    @Override
    public SortOrder[] requiredOrdering() {
        SortOrder[] ordering = writeRequirement.ordering();
        LOG.info("Requesting {} as write ordering for table {}", ordering, table.name());
        return ordering;
    }

    @Override
    public BatchWrite toBatch() {
        return new PaimonBatchWrite();
    }

    private class PaimonBatchWrite implements BatchWrite {

        @Override
        public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
            return new WriterFactory(table.rowType(), writeBuilder);
        }

        @Override
        public boolean useCommitCoordinator() {
            return false;
        }

        @Override
        public void commit(WriterCommitMessage[] messages) {
            LOG.info("Committing to table {}, ", table.name());
            BatchTableCommit batchTableCommit = writeBuilder.newCommit();
            List<CommitMessage> allCommitMessage = Lists.newArrayList();

            for (WriterCommitMessage message : messages) {
                if (message != null) {
                    List<CommitMessage> commitMessages = ((TaskCommit) message).commitMessages();
                    allCommitMessage.addAll(commitMessages);
                }
            }

            try {
                long start = System.currentTimeMillis();
                batchTableCommit.commit(allCommitMessage);
                LOG.info("Committed in {} ms", System.currentTimeMillis() - start);
                batchTableCommit.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void abort(WriterCommitMessage[] messages) {
            // TODO abort
        }
    }

    @Override
    public String toString() {
        return String.format(
                "PaimonWrite(table=%s, %s)",
                table.name(),
                overwriteDynamic
                        ? "overwriteDynamic=true"
                        : String.format("overwritePartitions=%s", overwritePartitions));
    }

    private static class WriterFactory implements DataWriterFactory {
        private final RowType rowType;
        private final BatchWriteBuilder batchWriteBuilder;

        WriterFactory(RowType rowType, BatchWriteBuilder batchWriteBuilder) {
            this.rowType = rowType;
            this.batchWriteBuilder = batchWriteBuilder;
        }

        @Override
        public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
            BatchTableWrite batchTableWrite = batchWriteBuilder.newWrite();
            return new GenericWriter(batchTableWrite, rowType);
        }
    }

    private static class GenericWriter implements DataWriter<InternalRow> {
        private final BatchTableWrite batchTableWrite;
        private final RowType rowType;
        private final IOManager ioManager;

        private GenericWriter(BatchTableWrite batchTableWrite, RowType rowType) {
            this.batchTableWrite = batchTableWrite;
            this.rowType = rowType;
            this.ioManager = SparkUtils.createIOManager();
            batchTableWrite.withIOManager(ioManager);
        }

        @Override
        public void write(InternalRow record) throws IOException {
            // TODO rowKind
            SparkInternalRowWrapper wrappedInternalRow =
                    new SparkInternalRowWrapper(rowType, record);
            try {
                batchTableWrite.write(wrappedInternalRow);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public WriterCommitMessage commit() throws IOException {
            try {
                List<CommitMessage> commitMessages = batchTableWrite.prepareCommit();
                TaskCommit taskCommit = new TaskCommit(commitMessages);
                return taskCommit;
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                close();
            }
        }

        @Override
        public void abort() throws IOException {
            // TODO clean uncommitted files
            close();
        }

        @Override
        public void close() throws IOException {
            try {
                batchTableWrite.close();
                ioManager.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class TaskCommit implements WriterCommitMessage {
        private final List<byte[]> serializedMessageList = Lists.newArrayList();

        TaskCommit(List<CommitMessage> commitMessages) {
            if (commitMessages == null || commitMessages.isEmpty()) {
                return;
            }

            CommitMessageSerializer serializer = new CommitMessageSerializer();
            for (CommitMessage commitMessage : commitMessages) {
                try {
                    serializedMessageList.add(serializer.serialize(commitMessage));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        List<CommitMessage> commitMessages() {
            CommitMessageSerializer deserializer = new CommitMessageSerializer();
            List<CommitMessage> commitMessageList = Lists.newArrayList();
            for (byte[] serializedMessage : serializedMessageList) {
                try {
                    commitMessageList.add(
                            deserializer.deserialize(deserializer.getVersion(), serializedMessage));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            return commitMessageList;
        }
    }
}
