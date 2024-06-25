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

package org.apache.paimon.flink.utils;

import org.apache.paimon.options.MemorySize;

import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

/** Utils for using Flink managed memory. */
public class ManagedMemoryUtils {

    public static void declareManagedMemory(DataStream<?> dataStream, MemorySize memorySize) {
        dataStream
                .getTransformation()
                .declareManagedMemoryUseCaseAtOperatorScope(
                        ManagedMemoryUseCase.OPERATOR, memorySize.getMebiBytes());
    }

    public static long computeManagedMemory(AbstractStreamOperator<?> operator) {
        final Environment environment = operator.getContainingTask().getEnvironment();
        return environment
                .getMemoryManager()
                .computeMemorySize(
                        operator.getOperatorConfig()
                                .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                        ManagedMemoryUseCase.OPERATOR,
                                        environment.getJobConfiguration(),
                                        environment.getTaskManagerInfo().getConfiguration(),
                                        environment.getUserCodeClassLoader().asClassLoader()));
    }
}
