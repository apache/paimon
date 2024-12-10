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

package org.apache.paimon.flink;

import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A stream source that: 1) emits a list of elements without allowing checkpoints, 2) then waits for
 * two more checkpoints to complete, 3) then re-emits the same elements before 4) waiting for
 * another two checkpoints and 5) exiting.
 *
 * <p>The reason this class is rewritten is to support {@link CheckpointedFunction}.
 */
public class FiniteTestSource<T> extends AbstractNonCoordinatedSource<T> {

    private static final long serialVersionUID = 1L;

    private final List<T> elements;

    private final boolean emitOnce;

    public FiniteTestSource(List<T> elements, boolean emitOnce) {
        this.elements = elements;
        this.emitOnce = emitOnce;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, SimpleSourceSplit> createReader(SourceReaderContext sourceReaderContext)
            throws Exception {
        return new Reader<>(elements, emitOnce);
    }

    private static class Reader<T> extends AbstractNonCoordinatedSourceReader<T> {

        private final List<T> elements;

        private final boolean emitOnce;

        private int numTimesEmitted = 0;

        private int numCheckpointsComplete;

        private Integer checkpointToAwait;

        private Reader(List<T> elements, boolean emitOnce) {
            this.elements = elements;
            this.emitOnce = emitOnce;
            this.numCheckpointsComplete = 0;
        }

        @Override
        public synchronized InputStatus pollNext(ReaderOutput<T> readerOutput) {
            if (checkpointToAwait == null) {
                checkpointToAwait = numCheckpointsComplete + 2;
            }
            switch (numTimesEmitted) {
                case 0:
                    emitElements(readerOutput, false);
                    if (numCheckpointsComplete < checkpointToAwait) {
                        return InputStatus.MORE_AVAILABLE;
                    }
                    emitElements(readerOutput, true);
                    if (numCheckpointsComplete < checkpointToAwait + 2) {
                        return InputStatus.MORE_AVAILABLE;
                    }
                    break;
                case 1:
                    emitElements(readerOutput, true);
                    if (numCheckpointsComplete < checkpointToAwait) {
                        return InputStatus.MORE_AVAILABLE;
                    }
                    break;
                case 2:
                    // Maybe missed notifyCheckpointComplete, wait next notifyCheckpointComplete
                    if (numCheckpointsComplete < checkpointToAwait) {
                        return InputStatus.MORE_AVAILABLE;
                    }
                    break;
            }
            return InputStatus.END_OF_INPUT;
        }

        @Override
        public void addSplits(List<SimpleSourceSplit> list) {
            List<Integer> retrievedStates =
                    list.stream()
                            .map(x -> Integer.parseInt(x.value()))
                            .collect(Collectors.toList());

            // given that the parallelism of the function is 1, we can only have 1 state
            Preconditions.checkArgument(
                    retrievedStates.size() == 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            numTimesEmitted = retrievedStates.get(0);
            Preconditions.checkArgument(
                    numTimesEmitted <= 2,
                    getClass().getSimpleName()
                            + " retrieved invalid numTimesEmitted: "
                            + numTimesEmitted);
        }

        @Override
        public List<SimpleSourceSplit> snapshotState(long l) {
            return Collections.singletonList(
                    new SimpleSourceSplit(Integer.toString(numTimesEmitted)));
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            numCheckpointsComplete++;
        }

        private void emitElements(ReaderOutput<T> readerOutput, boolean isSecond) {
            if (!isSecond || !emitOnce) {
                for (T t : elements) {
                    readerOutput.collect(t);
                }
            }
            numTimesEmitted++;
        }
    }
}
