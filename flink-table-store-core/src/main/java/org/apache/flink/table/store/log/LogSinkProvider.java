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

package org.apache.flink.table.store.log;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.store.table.sink.SinkRecord;

import java.io.Serializable;
import java.util.function.Consumer;

/** A {@link Serializable} sink provider for log store. */
public interface LogSinkProvider extends Serializable {

    /** Creates a {@link Sink} instance. */
    Sink<SinkRecord> createSink();

    /**
     * Create a metadata consumer for {@link Sink.InitContext#metadataConsumer()} from {@link
     * WriteCallback}.
     */
    Consumer<?> createMetadataConsumer(WriteCallback callback);

    /**
     * A callback interface that the user can implement to know the offset of the bucket when the
     * request is complete.
     */
    interface WriteCallback {

        /**
         * A callback method the user can implement to provide asynchronous handling of request
         * completion. This method will be called when the record sent to the server has been
         * acknowledged.
         */
        void onCompletion(int bucket, long offset);
    }
}
