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

package org.apache.paimon.service.network;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.service.messages.GlobalIndexRequest;
import org.apache.paimon.service.messages.GlobalIndexResponse;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.apache.paimon.service.messages.KvRequestTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests cancellation cleanup while a server connection is still being established. */
class ServerConnectionTest {

    @Test
    void testCancelledRequestsAreRemovedFromPendingConnectionQueue() {
        ServerConnection<GlobalIndexRequest, GlobalIndexResponse> connection =
                ServerConnection.createPendingConnection(
                        "Pending Cancellation Test Client",
                        new MessageSerializer<>(
                                new GlobalIndexRequest.Deserializer(),
                                new GlobalIndexResponse.Deserializer()),
                        new DisabledServiceRequestStats());

        for (int i = 0; i < 100; i++) {
            CompletableFuture<GlobalIndexResponse> future =
                    connection.sendRequest(
                            new GlobalIndexRequest("epoch", 1L, new BinaryRow[] {row(i)}));
            assertThat(future.cancel(false)).isTrue();
        }

        assertThat(connection.numPendingRequests()).isZero();
        connection.close();
    }
}
