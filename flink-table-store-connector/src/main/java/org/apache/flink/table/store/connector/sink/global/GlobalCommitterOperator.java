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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.util.function.SerializableSupplier;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An {@link AbstractCommitterOperator} to process global committer. */
public class GlobalCommitterOperator<CommT, GlobalCommT>
        extends AbstractCommitterOperator<CommT, GlobalCommT> {

    private static final long serialVersionUID = 1L;

    private final SerializableSupplier<GlobalCommitter<CommT, GlobalCommT>> committerFactory;

    /**
     * Aggregate committables to global committables and commit the global committables to the
     * external system.
     */
    private GlobalCommitter<CommT, GlobalCommT> committer;

    public GlobalCommitterOperator(
            SerializableSupplier<GlobalCommitter<CommT, GlobalCommT>> committerFactory,
            SerializableSupplier<SimpleVersionedSerializer<GlobalCommT>> committableSerializer) {
        super(committableSerializer);
        this.committerFactory = checkNotNull(committerFactory);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        committer = committerFactory.get();
        super.initializeState(context);
    }

    @Override
    public void commit(boolean isRecover, List<GlobalCommT> committables)
            throws IOException, InterruptedException {
        if (isRecover) {
            committables = committer.filterRecoveredCommittables(committables);
        }
        committer.commit(committables);
    }

    @Override
    public List<GlobalCommT> toCommittables(long checkpoint, List<CommT> inputs) throws Exception {
        return Collections.singletonList(committer.combine(checkpoint, inputs));
    }

    @Override
    public void close() throws Exception {
        committer.close();
        super.close();
    }
}
