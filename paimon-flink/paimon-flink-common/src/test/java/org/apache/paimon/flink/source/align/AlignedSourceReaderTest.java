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

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.source.FileStoreSourceReader;
import org.apache.paimon.flink.source.FileStoreSourceReaderTest;
import org.apache.paimon.flink.source.TestChangelogDataReadWrite;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;

import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.table.data.RowData;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link AlignedSourceReader}. */
public class AlignedSourceReaderTest extends FileStoreSourceReaderTest {

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

    @Override
    @Ignore
    public void testReaderOnSplitFinished() throws Exception {
        // ignore
    }

    @Override
    protected FileStoreSourceReader createReader(TestingReaderContext context) {
        return new AlignedSourceReader(
                context,
                new TestChangelogDataReadWrite(tempDir.toString()).createReadWithKey(),
                new FileStoreSourceReaderMetrics(new DummyMetricGroup()),
                IOManager.create(tempDir.toString()),
                null,
                new FutureCompletingBlockingQueue<>(2),
                null);
    }
}
