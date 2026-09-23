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

package org.apache.paimon.tests;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SparkE2eTest#stripTrailingSparkErrorLogs(String)}. Does not need Docker. */
public class SparkE2eOutputTest {

    private static final String ROWS = "1\t10\tHi\n2\t20\tHello\n3\t30\tTable\n";

    private static final String COARSE_GRAINED_SCHEDULER_ERROR =
            "26/07/29 04:25:43 ERROR Utils: Uncaught exception in thread dispatcher-CoarseGrainedScheduler\n"
                    + "org.apache.spark.SparkException: Could not find CoarseGrainedScheduler.\n"
                    + "\tat org.apache.spark.rpc.netty.Dispatcher.postMessage(Dispatcher.scala:178)\n";

    private static final String TRANSPORT_REQUEST_HANDLER_ERROR =
            "26/08/30 07:28:25 ERROR TransportRequestHandler: Error while invoking RpcHandler#receive() for one-way message.\n"
                    + "org.apache.spark.SparkException: Could not find AppClient.\n"
                    + "\tat org.apache.spark.rpc.netty.Dispatcher.postMessage(Dispatcher.scala:178)\n"
                    + "\tat java.lang.Thread.run(Thread.java:748)\n";

    @Test
    public void testNoErrorLogIsUnchanged() {
        assertThat(SparkE2eTest.stripTrailingSparkErrorLogs(ROWS)).isEqualTo(ROWS);
    }

    @Test
    public void testStripsCoarseGrainedSchedulerError() {
        assertThat(SparkE2eTest.stripTrailingSparkErrorLogs(ROWS + COARSE_GRAINED_SCHEDULER_ERROR))
                .isEqualTo(ROWS);
    }

    @Test
    public void testStripsRepeatedTransportRequestHandlerErrors() {
        assertThat(
                        SparkE2eTest.stripTrailingSparkErrorLogs(
                                ROWS
                                        + TRANSPORT_REQUEST_HANDLER_ERROR
                                        + TRANSPORT_REQUEST_HANDLER_ERROR))
                .isEqualTo(ROWS);
    }

    @Test
    public void testErrorOnFirstLineYieldsEmptyOutput() {
        assertThat(SparkE2eTest.stripTrailingSparkErrorLogs(TRANSPORT_REQUEST_HANDLER_ERROR))
                .isEmpty();
    }

    @Test
    public void testErrorWordInsideResultRowIsKept() {
        String rows = "1\t10\tsome ERROR text\n2\t20\tERROR Foo\n";
        assertThat(SparkE2eTest.stripTrailingSparkErrorLogs(rows)).isEqualTo(rows);
    }
}
