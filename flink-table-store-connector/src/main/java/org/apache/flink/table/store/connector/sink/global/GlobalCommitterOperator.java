/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.connector.sink.global;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collections;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** An operator that processes committables of a {@link Sink}. */
public class GlobalCommitterOperator<CommT, GlobalCommT> extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<CommittableMessage<CommT>, Void>, BoundedOneInput {

    private final GlobalCommitterHandler<CommT, GlobalCommT> committerHandler;

    public GlobalCommitterOperator(GlobalCommitterHandler<CommT, GlobalCommT> committerHandler) {
        this.committerHandler = checkNotNull(committerHandler);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        committerHandler.initializeState(context);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        committerHandler.snapshotState(context);
    }

    @Override
    public void endInput() throws Exception {
        committerHandler.endOfInput();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        committerHandler.notifyCheckpointCompleted(checkpointId);
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<CommT>> element) {
        CommittableMessage<CommT> message = element.getValue();
        if (message instanceof CommittableWithLineage) {
            committerHandler.processCommittables(
                    Collections.singletonList(
                            ((CommittableWithLineage<CommT>) message).getCommittable()));
        }
    }

    @Override
    public void close() throws Exception {
        closeAll(committerHandler, super::close);
    }
}
