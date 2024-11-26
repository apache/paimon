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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.utils.TestingMetricUtils;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.function.SupplierWithException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.CoreOptions.CONSUMER_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MonitorFunction} and {@link ReadOperator}. */
public class OperatorSourceTest {

    @TempDir Path tempDir;

    private Table table;

    @BeforeEach
    public void before()
            throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException,
                    Catalog.TableNotExistException, Catalog.DatabaseAlreadyExistException {
        Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(new org.apache.paimon.fs.Path(tempDir.toUri())));
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .primaryKey("a")
                        .option(CONSUMER_ID.key(), "my_consumer")
                        .option("bucket", "1")
                        .build();
        Identifier identifier = Identifier.create("default", "t");
        catalog.createDatabase("default", false);
        catalog.createTable(identifier, schema, false);
        this.table = catalog.getTable(identifier);
    }

    private void writeToTable(int a, int b, int c) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        write.write(GenericRow.of(a, b, c));
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(write.prepareCommit());
        write.close();
        commit.close();
    }

    private List<List<Integer>> readSplit(Split split) throws IOException {
        TableRead read = table.newReadBuilder().newRead();
        List<List<Integer>> result = new ArrayList<>();
        read.createReader(split)
                .forEachRemaining(
                        row ->
                                result.add(
                                        Arrays.asList(
                                                row.getInt(0), row.getInt(1), row.getInt(2))));
        return result;
    }

    @Test
    public void testMonitorFunction() throws Exception {
        // 1. run first
        OperatorSubtaskState snapshot;
        {
            MonitorFunction function = new MonitorFunction(table.newReadBuilder(), 10, false);
            StreamSource<Split, MonitorFunction> src = new StreamSource<>(function);
            AbstractStreamOperatorTestHarness<Split> testHarness =
                    new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);
            testHarness.open();
            snapshot = testReadSplit(function, () -> testHarness.snapshot(0, 0), 1, 1, 1);
        }

        // 2. restore from state
        {
            MonitorFunction functionCopy1 = new MonitorFunction(table.newReadBuilder(), 10, false);
            StreamSource<Split, MonitorFunction> srcCopy1 = new StreamSource<>(functionCopy1);
            AbstractStreamOperatorTestHarness<Split> testHarnessCopy1 =
                    new AbstractStreamOperatorTestHarness<>(srcCopy1, 1, 1, 0);
            testHarnessCopy1.initializeState(snapshot);
            testHarnessCopy1.open();
            testReadSplit(
                    functionCopy1,
                    () -> {
                        testHarnessCopy1.snapshot(1, 1);
                        testHarnessCopy1.notifyOfCompletedCheckpoint(1);
                        return null;
                    },
                    2,
                    2,
                    2);
        }

        // 3. restore from consumer id
        {
            MonitorFunction functionCopy2 = new MonitorFunction(table.newReadBuilder(), 10, false);
            StreamSource<Split, MonitorFunction> srcCopy2 = new StreamSource<>(functionCopy2);
            AbstractStreamOperatorTestHarness<Split> testHarnessCopy2 =
                    new AbstractStreamOperatorTestHarness<>(srcCopy2, 1, 1, 0);
            testHarnessCopy2.open();
            testReadSplit(functionCopy2, () -> null, 3, 3, 3);
        }
    }

    @Test
    public void testReadOperator() throws Exception {
        ReadOperator readOperator = new ReadOperator(table.newReadBuilder());
        OneInputStreamOperatorTestHarness<Split, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(readOperator);
        harness.setup(
                InternalSerializers.create(
                        RowType.of(new IntType(), new IntType(), new IntType())));
        writeToTable(1, 1, 1);
        writeToTable(2, 2, 2);
        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        harness.open();
        for (Split split : splits) {
            harness.processElement(new StreamRecord<>(split));
        }
        ArrayList<Object> values = new ArrayList<>(harness.getOutput());
        assertThat(values)
                .containsExactlyInAnyOrder(
                        new StreamRecord<>(GenericRowData.of(1, 1, 1)),
                        new StreamRecord<>(GenericRowData.of(2, 2, 2)));
    }

    @Test
    public void testReadOperatorMetricsRegisterAndUpdate() throws Exception {
        ReadOperator readOperator = new ReadOperator(table.newReadBuilder());
        OneInputStreamOperatorTestHarness<Split, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(readOperator);
        harness.setup(
                InternalSerializers.create(
                        RowType.of(new IntType(), new IntType(), new IntType())));
        writeToTable(1, 1, 1);
        writeToTable(2, 2, 2);
        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        assertThat(splits.size()).isGreaterThan(0);
        MetricGroup readerOperatorMetricGroup = readOperator.getMetricGroup();
        harness.open();
        assertThat(
                        TestingMetricUtils.getGauge(
                                        readerOperatorMetricGroup, "currentFetchEventTimeLag")
                                .getValue())
                .isEqualTo(-1L);
        assertThat(
                        TestingMetricUtils.getGauge(
                                        readerOperatorMetricGroup, "currentEmitEventTimeLag")
                                .getValue())
                .isEqualTo(-1L);

        Thread.sleep(300L);
        assertThat((Long)TestingMetricUtils.getGauge(
                        readerOperatorMetricGroup, "sourceIdleTime")
                .getValue()).isGreaterThan(299L);

        harness.processElement(new StreamRecord<>(splits.get(0)));
        assertThat(
                        (Long)
                                TestingMetricUtils.getGauge(
                                                readerOperatorMetricGroup,
                                                "currentFetchEventTimeLag")
                                        .getValue())
                .isGreaterThan(0);
        long emitEventTimeLag =
                (Long)
                        TestingMetricUtils.getGauge(
                                        readerOperatorMetricGroup, "currentEmitEventTimeLag")
                                .getValue();
        assertThat(emitEventTimeLag).isGreaterThan(0);

        // wait for a while and read metrics again, metrics should not change
        Thread.sleep(100);
        assertThat(
                        (Long)
                                TestingMetricUtils.getGauge(
                                                readerOperatorMetricGroup,
                                                "currentEmitEventTimeLag")
                                        .getValue())
                .isEqualTo(emitEventTimeLag);

        assertThat((Long)TestingMetricUtils.getGauge(
                        readerOperatorMetricGroup, "sourceIdleTime")
                .getValue()).isGreaterThan(99L).isLessThan(300L);

    }

    private <T> T testReadSplit(
            MonitorFunction function,
            SupplierWithException<T, Exception> beforeClose,
            int a,
            int b,
            int c)
            throws Exception {
        Throwable[] error = new Throwable[1];
        ArrayBlockingQueue<Split> queue = new ArrayBlockingQueue<>(10);

        DummySourceContext sourceContext =
                new DummySourceContext() {
                    @Override
                    public void collect(Split element) {
                        queue.add(element);
                    }
                };

        Thread runner =
                new Thread(
                        () -> {
                            try {
                                function.run(sourceContext);
                            } catch (Throwable t) {
                                t.printStackTrace();
                                error[0] = t;
                            }
                        });
        runner.start();

        writeToTable(a, b, c);

        Split split = queue.poll(1, TimeUnit.MINUTES);
        assertThat(readSplit(split)).containsExactlyInAnyOrder(Arrays.asList(a, b, c));

        T t = beforeClose.get();
        function.cancel();
        runner.join();

        assertThat(error[0]).isNull();

        return t;
    }

    private abstract static class DummySourceContext
            implements SourceFunction.SourceContext<Split> {

        private final Object lock = new Object();

        @Override
        public void collectWithTimestamp(Split element, long timestamp) {}

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void markAsTemporarilyIdle() {}

        @Override
        public Object getCheckpointLock() {
            return lock;
        }

        @Override
        public void close() {}
    }
}
