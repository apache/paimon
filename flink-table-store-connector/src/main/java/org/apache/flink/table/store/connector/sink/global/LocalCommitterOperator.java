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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommitRequestImpl;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommitRequestState;
import org.apache.flink.util.function.SerializableSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link AbstractCommitterOperator} to process local committer. */
public class LocalCommitterOperator<CommT> extends AbstractCommitterOperator<CommT, CommT> {

    private static final long serialVersionUID = 1L;

    private final SerializableSupplier<Committer<CommT>> committerFactory;

    private Committer<CommT> committer;

    public LocalCommitterOperator(
            SerializableSupplier<Committer<CommT>> committerFactory,
            SerializableSupplier<SimpleVersionedSerializer<CommT>> committableSerializer) {
        super(committableSerializer);
        this.committerFactory = checkNotNull(committerFactory);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        committer = committerFactory.get();
        super.initializeState(context);
    }

    @Override
    public void commit(boolean isRecover, List<CommT> committables)
            throws IOException, InterruptedException {
        if (committables.isEmpty()) {
            return;
        }

        List<CommitRequestImpl> requests = new ArrayList<>(committables.size());
        for (CommT comm : committables) {
            requests.add(new CommitRequestImpl(comm));
        }

        long sleep = 1000;
        while (true) {
            // commit
            requests.forEach(CommitRequestImpl::setSelected);
            committer.commit(new ArrayList<>(requests));
            requests.forEach(CommitRequestImpl::setCommittedIfNoError);

            // drain finished
            requests.removeIf(CommitRequestImpl::isFinished);
            if (requests.isEmpty()) {
                return;
            }

            //noinspection BusyWait
            Thread.sleep(sleep);
            sleep *= 2;
        }
    }

    @Override
    public List<CommT> toCommittables(long checkpoint, List<CommT> inputs) {
        return inputs;
    }

    @Override
    public void close() throws Exception {
        committer.close();
        super.close();
    }

    /** {@link CommitRequest} implementation. */
    public class CommitRequestImpl implements CommitRequest<CommT> {

        private CommT committable;
        private int numRetries;
        private CommitRequestState state;

        private CommitRequestImpl(CommT committable) {
            this.committable = committable;
            this.state = CommitRequestState.RECEIVED;
        }

        private boolean isFinished() {
            return state.isFinalState();
        }

        @Override
        public CommT getCommittable() {
            return this.committable;
        }

        @Override
        public int getNumberOfRetries() {
            return this.numRetries;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            this.state = CommitRequestState.FAILED;
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            this.state = CommitRequestState.FAILED;
            throw new IllegalStateException("Failed to commit " + this.committable, t);
        }

        @Override
        public void retryLater() {
            this.state = CommitRequestState.RETRY;
            ++this.numRetries;
        }

        @Override
        public void updateAndRetryLater(CommT committable) {
            this.committable = committable;
            this.retryLater();
        }

        @Override
        public void signalAlreadyCommitted() {
            this.state = CommitRequestState.COMMITTED;
        }

        void setSelected() {
            state = CommitRequestState.RECEIVED;
        }

        void setCommittedIfNoError() {
            if (state == CommitRequestState.RECEIVED) {
                state = CommitRequestState.COMMITTED;
            }
        }
    }

    /** Convert a {@link CommitRequest} to another type. */
    public static <CommT, NewT> CommitRequest<NewT> convertCommitRequest(
            CommitRequest<CommT> request, Function<CommT, NewT> to, Function<NewT, CommT> from) {
        return new CommitRequest<NewT>() {

            @Override
            public NewT getCommittable() {
                return to.apply(request.getCommittable());
            }

            @Override
            public int getNumberOfRetries() {
                return request.getNumberOfRetries();
            }

            @Override
            public void signalFailedWithKnownReason(Throwable throwable) {
                request.signalFailedWithKnownReason(throwable);
            }

            @Override
            public void signalFailedWithUnknownReason(Throwable throwable) {
                request.signalFailedWithUnknownReason(throwable);
            }

            @Override
            public void retryLater() {
                request.retryLater();
            }

            @Override
            public void updateAndRetryLater(NewT committable) {
                request.updateAndRetryLater(from.apply(committable));
            }

            @Override
            public void signalAlreadyCommitted() {
                request.signalAlreadyCommitted();
            }
        };
    }
}
