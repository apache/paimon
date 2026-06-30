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

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.ThreadFactory;

/**
 * Single-thread executor factory used by the {@link WriteOperatorCoordinator} to run commit work
 * off the JM main thread. Any uncaught exception fails the job via {@link
 * OperatorCoordinator.Context#failJob(Throwable)}.
 *
 * <p>Adapted from {@code
 * org.apache.flink.runtime.source.coordinator.SourceCoordinatorProvider.CoordinatorExecutorThreadFactory}.
 */
public class CoordinatorExecutorThreadFactory
        implements ThreadFactory, Thread.UncaughtExceptionHandler {

    private static final Logger LOG =
            LoggerFactory.getLogger(CoordinatorExecutorThreadFactory.class);

    private final String coordinatorThreadName;
    private final ClassLoader contextClassLoader;
    private final Thread.UncaughtExceptionHandler errorHandler;

    @Nullable private Thread coordinatorThread;

    public CoordinatorExecutorThreadFactory(
            String coordinatorThreadName, OperatorCoordinator.Context context) {
        this(
                coordinatorThreadName,
                context.getUserCodeClassloader(),
                (t, e) -> {
                    LOG.error(
                            "Thread '{}' produced an uncaught exception. Failing the job.",
                            t.getName(),
                            e);
                    context.failJob(e);
                });
    }

    CoordinatorExecutorThreadFactory(
            String coordinatorThreadName,
            ClassLoader contextClassLoader,
            Thread.UncaughtExceptionHandler errorHandler) {
        this.coordinatorThreadName = coordinatorThreadName;
        this.contextClassLoader = contextClassLoader;
        this.errorHandler = errorHandler;
    }

    @Override
    public synchronized Thread newThread(Runnable r) {
        coordinatorThread = new Thread(r, coordinatorThreadName);
        coordinatorThread.setContextClassLoader(contextClassLoader);
        coordinatorThread.setUncaughtExceptionHandler(this);
        return coordinatorThread;
    }

    @Override
    public synchronized void uncaughtException(Thread t, Throwable e) {
        errorHandler.uncaughtException(t, e);
    }

    public String getCoordinatorThreadName() {
        return coordinatorThreadName;
    }

    public boolean isCurrentThreadCoordinatorThread() {
        return Thread.currentThread() == coordinatorThread;
    }
}
