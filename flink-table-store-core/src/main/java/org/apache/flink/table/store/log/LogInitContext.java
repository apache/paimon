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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

/** A {@link InitContext} with {@link InitContext#metadataConsumer()}. */
public class LogInitContext implements InitContext {

    private final InitContext context;
    private final Consumer<?> metaConsumer;

    public LogInitContext(InitContext context, Consumer<?> metaConsumer) {
        this.context = context;
        this.metaConsumer = metaConsumer;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return context.getUserCodeClassLoader();
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return context.getMailboxExecutor();
    }

    @Override
    public ProcessingTimeService getProcessingTimeService() {
        return context.getProcessingTimeService();
    }

    @Override
    public int getSubtaskId() {
        return context.getSubtaskId();
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return context.getNumberOfParallelSubtasks();
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return context.metricGroup();
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return context.getRestoredCheckpointId();
    }

    @Override
    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        return context.asSerializationSchemaInitializationContext();
    }

    @Override
    public Optional<Consumer<?>> metadataConsumer() {
        return Optional.of(metaConsumer);
    }
}
