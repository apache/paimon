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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.utils.InstantiationUtil;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Utilities for wrapping and unwrapping {@link CoordinationResponse} by {@link
 * CollectCoordinationResponse}.
 *
 * <p>This class is mostly copied from <a
 * href="https://github.com/apache/flink-cdc/blob/23a1c2efb6fa9ce1c9f17b3836f6aaa995bb0660/flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/operators/schema/common/CoordinationResponseUtils.java#L29">apache/flink-cdc</a>.
 *
 * <p>Currently, Flink's RPC gateway can only deserialize classes accessible from Flink's class
 * loader (see <a
 * href="https://github.com/apache/flink/blob/ce03ea11d10df28affa040847dbaf02680eb785d/flink-rpc/flink-rpc-akka/src/main/java/org/apache/flink/runtime/rpc/pekko/PekkoRpcService.java#L543">apache/flink</a>),
 * so we have to wrap our own response with a class ({@link CollectCoordinationResponse}) in Flink's
 * project.
 */
public class CoordinationResponseUtils {

    private static final String MAGIC_VERSION = "__paimon__";
    private static final long MAGIC_OFFSET = 20250627L;

    public static <R extends CoordinationResponse> CoordinationResponse wrap(R response) {
        CoordinationResponseSerializer serializer = new CoordinationResponseSerializer();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            serializer.serialize(response, new DataOutputViewStreamWrapper(out));
            return new CollectCoordinationResponse(
                    MAGIC_VERSION, MAGIC_OFFSET, Collections.singletonList(baos.toByteArray()));
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to wrap CoordinationResponse \"%s\" with type \"%s\"",
                            response, response.getClass().getCanonicalName()),
                    e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <R extends CoordinationResponse> R unwrap(CoordinationResponse response) {
        try {
            CollectCoordinationResponse rawResponse = (CollectCoordinationResponse) response;
            List<CoordinationResponse> results =
                    rawResponse.getResults(new CoordinationResponseSerializer());

            return (R) results.get(0);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to unwrap CoordinationResponse", e);
        }
    }

    private static class CoordinationResponseSerializer
            extends TypeSerializer<CoordinationResponse> {

        @Override
        public void serialize(CoordinationResponse record, DataOutputView target)
                throws IOException {
            byte[] serialized = InstantiationUtil.serializeObject(record);
            target.writeInt(serialized.length);
            target.write(serialized);
        }

        @Override
        public CoordinationResponse deserialize(DataInputView source) throws IOException {
            try {
                int length = source.readInt();
                byte[] serialized = new byte[length];
                source.readFully(serialized);
                return InstantiationUtil.deserializeObject(
                        serialized, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unable to deserialize CoordinationResponse", e);
            }
        }

        @Override
        public CoordinationResponse deserialize(CoordinationResponse reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<CoordinationResponse> duplicate() {
            return new CoordinationResponseSerializer();
        }

        @Override
        public CoordinationResponse createInstance() {
            return new CoordinationResponse() {};
        }

        @Override
        public CoordinationResponse copy(CoordinationResponse from) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CoordinationResponse copy(CoordinationResponse from, CoordinationResponse reuse) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            CoordinationResponse deserialize = deserialize(source);
            serialize(deserialize, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CoordinationResponseSerializer;
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public TypeSerializerSnapshot<CoordinationResponse> snapshotConfiguration() {
            return new CoordinationResponseDeserializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class CoordinationResponseDeserializerSnapshot
                extends SimpleTypeSerializerSnapshot<CoordinationResponse> {

            public CoordinationResponseDeserializerSnapshot() {
                super(CoordinationResponseSerializer::new);
            }
        }
    }
}
