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

package org.apache.paimon.flink.action;

import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.operation.DeleteOrderingGuardRescale;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Bounded native rescale tasks and a single atomic publisher for guarded tables. */
class DeleteOrderingGuardRescaleBuilder {

    private DeleteOrderingGuardRescaleBuilder() {}

    static void build(
            StreamExecutionEnvironment env,
            DeleteOrderingGuardRescale migration,
            List<DataSplit> splits,
            int parallelism)
            throws IOException {
        List<byte[]> tasks = new ArrayList<>();
        for (DataSplit split : splits) {
            DataOutputSerializer bytes = new DataOutputSerializer(256);
            split.serialize(bytes);
            tasks.add(bytes.getCopyOfBuffer());
        }
        env.fromCollection(tasks, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                .rebalance()
                .flatMap(new Rewrite(migration))
                .returns(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                .name("Rescale data and delete ordering guards")
                .setParallelism(parallelism)
                .transform(
                        "Publish guarded rescale",
                        Types.LONG,
                        new Commit(migration, "guard-rescale-" + UUID.randomUUID()))
                .setParallelism(1)
                .sinkTo(new DiscardingSink<>())
                .name("End rescale")
                .setParallelism(1);
    }

    private static class Rewrite implements FlatMapFunction<byte[], byte[]> {
        private static final long serialVersionUID = 1L;
        private final DeleteOrderingGuardRescale migration;

        private Rewrite(DeleteOrderingGuardRescale migration) {
            this.migration = migration;
        }

        @Override
        public void flatMap(byte[] value, Collector<byte[]> output) throws Exception {
            DataSplit split = DataSplit.deserialize(new DataInputDeserializer(value));
            CommitMessageSerializer serializer = new CommitMessageSerializer();
            for (CommitMessage message : migration.rewrite(split)) {
                output.collect(serializer.serialize(message));
            }
        }
    }

    private static class Commit extends BoundedOneInputOperator<byte[], Long> {
        private static final long serialVersionUID = 1L;
        private final DeleteOrderingGuardRescale migration;
        private final String commitUser;
        private transient List<CommitMessage> messages;
        private transient boolean committed;

        private Commit(DeleteOrderingGuardRescale migration, String commitUser) {
            this.migration = migration;
            this.commitUser = commitUser;
        }

        @Override
        public void open() throws Exception {
            super.open();
            messages = new ArrayList<>();
        }

        @Override
        public void processElement(StreamRecord<byte[]> element) throws Exception {
            CommitMessageSerializer serializer = new CommitMessageSerializer();
            messages.add(serializer.deserialize(serializer.getVersion(), element.getValue()));
        }

        @Override
        public void endInput() {
            migration.commit(commitUser, Long.MAX_VALUE, messages);
            committed = true;
            output.collect(new StreamRecord<>((long) messages.size()));
        }

        @Override
        public void close() throws Exception {
            try {
                if (!committed && messages != null) {
                    migration.abort(commitUser, Long.MAX_VALUE, messages);
                }
            } finally {
                super.close();
            }
        }
    }
}
