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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.FileStoreSourceReader;
import org.apache.paimon.flink.source.FileStoreSourceReaderTest;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplitGenerator;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.TableRead;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.paimon.utils.ReflectionUtils.getPrivateFieldValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link AlignedSourceReader}. */
public class AlignedSourceReaderTest extends FileStoreSourceReaderTest {

    private static final String COMMIT_USER = "commit_user";
    private static final String MAIN_OPERATOR_FIELD = "mainOperator";
    private static final String INPUT_PROCESSOR_FIELD = "inputProcessor";

    private FileStoreTable table;

    @BeforeEach
    @Override
    public void beforeEach() throws Exception {
        super.beforeEach();
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        TableSchema tableSchema = schemaManager.latest().get();
        this.table = FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
    }

    @Override
    @Test
    public void testAddMultipleSplits() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final AlignedSourceReader reader = (AlignedSourceReader) createReader(context);

        reader.start();
        assertThat(context.getNumSplitRequests()).isEqualTo(1);

        reader.addSplits(Arrays.asList(createTestFileSplit("id1"), createTestFileSplit("id2")));
        TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
        while (reader.getNumberOfCurrentlyAssignedSplits() > 0) {
            reader.pollNext(output);
            Thread.sleep(10);
        }
        // splits are only requested when a checkpoint is ready to be triggered
        assertThat(context.getNumSplitRequests()).isEqualTo(1);

        // prepare to trigger checkpoint
        reader.handleSourceEvents(new CheckpointEvent(1L));
        assertThat(reader.shouldTriggerCheckpoint()).isEqualTo(Optional.of(1L));
        assertThat(context.getNumSplitRequests()).isEqualTo(2);
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @Timeout(300)
    public void testCheckpointTrigger(boolean rpcFirst) throws Exception {
        writeTable();

        AlignedContinuousFileStoreSource alignedSource =
                new AlignedContinuousFileStoreSource(
                        table.newReadBuilder(), table.options(), null, false, null);
        SourceOperatorFactory<RowData> sourceOperatorFactory =
                new SourceOperatorFactory<>(alignedSource, WatermarkStrategy.noWatermarks());
        StreamTaskMailboxTestHarnessBuilder<RowData> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                SourceOperatorStreamTask::new,
                                InternalTypeInfo.of(
                                        LogicalTypeConversion.toLogicalType(table.rowType())))
                        .setCollectNetworkEvents()
                        .setupOutputForSingletonOperatorChain(sourceOperatorFactory);

        try (StreamTaskMailboxTestHarness<RowData> testHarness = builder.build()) {
            StreamTask<RowData, ?> streamTask = testHarness.getStreamTask();
            SourceOperator<RowData, FileStoreSourceSplit> sourceOperator =
                    getPrivateFieldValue(streamTask, MAIN_OPERATOR_FIELD);
            AlignedSourceReader sourceReader =
                    (AlignedSourceReader) sourceOperator.getSourceReader();
            StreamInputProcessor inputProcessor =
                    getPrivateFieldValue(streamTask, INPUT_PROCESSOR_FIELD);
            Queue<Object> output = testHarness.getOutput();

            // checkpoint meta
            CheckpointMetaData checkpointMetaData = new CheckpointMetaData(1L, 2);
            CheckpointOptions checkpointOptions =
                    CheckpointOptions.alignedNoTimeout(
                            CheckpointType.CHECKPOINT,
                            CheckpointStorageLocationReference.getDefault());

            // send splits firstly
            List<FileStoreSourceSplit> splits =
                    new FileStoreSourceSplitGenerator().createSplits(table.newStreamScan().plan());
            sourceOperator.handleOperatorEvent(
                    new AddSplitEvent<>(splits, alignedSource.getSplitSerializer()));
            SourceEventWrapper checkpointEvent = new SourceEventWrapper(new CheckpointEvent(1L));

            if (rpcFirst) {
                sourceOperator.handleOperatorEvent(checkpointEvent);
                streamTask.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
            }

            while (sourceReader.getNumberOfCurrentlyAssignedSplits() > 0
                    || inputProcessor.isAvailable()) {
                testHarness.processAll();
            }

            if (!rpcFirst) {
                sourceOperator.handleOperatorEvent(checkpointEvent);
                testHarness.processAll();
                CompletableFuture<Boolean> triggerCheckpointAsync =
                        streamTask.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
                // process mails until checkpoint has been processed
                while (!triggerCheckpointAsync.isDone()) {
                    testHarness.processSingleStep();
                }
            }

            assertThat(output.size()).isEqualTo(2);
            assertThat(output.peek()).isInstanceOf(StreamRecord.class);
            RowData record = ((StreamRecord<RowData>) output.poll()).getValue();
            assertThat(record)
                    .matches(
                            rowData ->
                                    !rowData.isNullAt(0)
                                            && !rowData.isNullAt(1)
                                            && !rowData.isNullAt(2));
            assertThat(record)
                    .matches(
                            rowData ->
                                    rowData.getLong(0) == 3L
                                            && rowData.getLong(1) == 33L
                                            && rowData.getInt(2) == 303);

            assertThat(output.peek()).isInstanceOf(CheckpointBarrier.class);
            assertThat(output.poll())
                    .isEqualTo(
                            new CheckpointBarrier(
                                    checkpointMetaData.getCheckpointId(),
                                    checkpointMetaData.getTimestamp(),
                                    checkpointOptions));
        }
    }

    @Override
    @Ignore
    public void testReaderOnSplitFinished() throws Exception {
        // ignore
    }

    @Override
    protected FileStoreSourceReader createReader(
            TestingReaderContext context, TableRead tableRead) {
        return new AlignedSourceReader(
                context,
                tableRead,
                new FileStoreSourceReaderMetrics(new DummyMetricGroup()),
                IOManager.create(tempDir.toString()),
                null,
                null,
                null);
    }

    private void writeTable() throws Exception {
        try (StreamTableWrite write = table.newWrite(COMMIT_USER);
                StreamTableCommit commit = table.newCommit(COMMIT_USER)) {

            write.write(GenericRow.of(3L, 33L, 303), 0);
            commit.commit(1, write.prepareCommit(true, 0));
        }
    }
}
