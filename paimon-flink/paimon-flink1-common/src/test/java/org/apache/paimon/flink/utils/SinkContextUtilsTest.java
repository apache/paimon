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

import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SinkContextUtils}. */
class SinkContextUtilsTest {

    @Test
    void testAttemptNumberFromInitContext() {
        InitContext context = context(InitContext.class, 3);

        assertThat(SinkContextUtils.getAttemptNumber(context)).isEqualTo(3);
    }

    @Test
    void testAttemptNumberFromWriterInitContext() {
        WriterInitContext context = context(WriterInitContext.class, 7);

        assertThat(SinkContextUtils.getAttemptNumber(context)).isEqualTo(7);
    }

    private <T> T context(Class<T> contextClass, int attemptNumber) {
        return contextClass.cast(
                Proxy.newProxyInstance(
                        contextClass.getClassLoader(),
                        new Class<?>[] {contextClass},
                        (proxy, method, arguments) -> {
                            if ("getAttemptNumber".equals(method.getName())) {
                                return attemptNumber;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        }));
    }
}
