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

package org.apache.flink.table.store.connector.sink.global;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * A {@link Sink} for exactly-once semantics using a two-phase commit protocol. The {@link Sink}
 * consists of a {@link SinkWriter} that performs the precommits and a {@link GlobalCommitter} that
 * actually commits the data.
 *
 * @param <InputT> The type of the sink's input
 * @param <CommT> The type of the committables.
 * @param <GlobalCommT> The type of the aggregated committable.
 */
public interface GlobalCommittingSink<InputT, CommT, GlobalCommT>
        extends TwoPhaseCommittingSink<InputT, CommT> {

    /**
     * Creates a {@link GlobalCommitter} that permanently makes the previously written data visible
     * through {@link GlobalCommitter#commit}.
     */
    GlobalCommitter<CommT, GlobalCommT> createGlobalCommitter();

    /** Returns the serializer of the global committable type. */
    SimpleVersionedSerializer<GlobalCommT> getGlobalCommittableSerializer();
}
