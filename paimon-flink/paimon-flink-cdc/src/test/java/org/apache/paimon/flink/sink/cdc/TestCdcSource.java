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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Testing parallel {@link org.apache.flink.api.connector.source.Source} to produce {@link
 * TestCdcEvent}. {@link TestCdcEvent}s with the same key will be produced by the same parallelism.
 */
public class TestCdcSource extends AbstractNonCoordinatedSource<TestCdcEvent> {

    private static final long serialVersionUID = 1L;
    private final LinkedList<TestCdcEvent> events;

    public TestCdcSource(Collection<TestCdcEvent> events) {
        this.events = new LinkedList<>(events);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<TestCdcEvent, SimpleSourceSplit> createReader(SourceReaderContext context) {
        return new Reader(
                context.getIndexOfSubtask(),
                context.currentParallelism(),
                new LinkedList<>(events));
    }

    private static class Reader extends AbstractNonCoordinatedSourceReader<TestCdcEvent> {
        private final int subtaskId;
        private final int totalSubtasks;

        private final LinkedList<TestCdcEvent> events;

        private final int numRecordsPerCheckpoint;
        private final AtomicInteger recordsThisCheckpoint;

        private Reader(int subtaskId, int totalSubtasks, LinkedList<TestCdcEvent> events) {
            this.subtaskId = subtaskId;
            this.totalSubtasks = totalSubtasks;
            this.events = events;
            numRecordsPerCheckpoint =
                    events.size() / ThreadLocalRandom.current().nextInt(10, 20) + 1;
            recordsThisCheckpoint = new AtomicInteger(0);
        }

        @Override
        public InputStatus pollNext(ReaderOutput<TestCdcEvent> readerOutput) throws Exception {
            if (events.isEmpty()) {
                return InputStatus.END_OF_INPUT;
            }

            if (recordsThisCheckpoint.get() >= numRecordsPerCheckpoint) {
                Thread.sleep(10);
                return InputStatus.MORE_AVAILABLE;
            }

            TestCdcEvent event = events.poll();
            if (event.records() != null) {
                if (Math.abs(event.hashCode()) % totalSubtasks != subtaskId) {
                    return InputStatus.MORE_AVAILABLE;
                }
            }
            readerOutput.collect(event);
            recordsThisCheckpoint.incrementAndGet();
            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<SimpleSourceSplit> snapshotState(long l) {
            recordsThisCheckpoint.set(0);
            return Collections.singletonList(
                    new SimpleSourceSplit(Integer.toString(events.size())));
        }

        @Override
        public void addSplits(List<SimpleSourceSplit> list) {
            int count =
                    list.stream()
                            .map(x -> Integer.parseInt(x.value()))
                            .reduce(Integer::sum)
                            .orElse(0);
            while (events.size() > count) {
                events.poll();
            }
        }
    }
}
