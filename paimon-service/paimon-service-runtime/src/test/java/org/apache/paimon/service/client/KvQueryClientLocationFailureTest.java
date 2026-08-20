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

package org.apache.paimon.service.client;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.query.QueryLocation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KvQueryClient} when the query location lookup fails. */
public class KvQueryClientLocationFailureTest {

    private static final String LOCATION_FAILURE = "Cannot find address for table path: /test/path";

    private KvQueryClient client;

    @AfterEach
    public void afterEach() {
        if (client != null) {
            client.shutdown();
            client = null;
        }
    }

    /**
     * The retried location lookup happens inside a callback of the previous request, so a
     * synchronous throw there must be turned into an exceptionally completed future. Otherwise the
     * future returned by {@link KvQueryClient#getValues} is never completed at all.
     */
    @Test
    public void testLocationLookupThrowingOnRetryCompletesFuture() throws Exception {
        InetSocketAddress unreachable = unusedLocalAddress();
        AtomicInteger lookups = new AtomicInteger();
        QueryLocation queryLocation =
                (partition, bucket, forceUpdate) -> {
                    lookups.incrementAndGet();
                    if (forceUpdate) {
                        // this is what QueryLocationImpl does once the service file is gone
                        throw new RuntimeException(LOCATION_FAILURE);
                    }
                    // valid address, but nothing listens on it: the connect fails with a
                    // ConnectException, which triggers the forced location update above
                    return unreachable;
                };

        client = new KvQueryClient(queryLocation, 1);
        CompletableFuture<BinaryRow[]> future =
                client.getValues(BinaryRow.EMPTY_ROW, 0, new BinaryRow[] {BinaryRow.EMPTY_ROW});

        assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseMessage(LOCATION_FAILURE);
        assertThat(lookups.get()).isEqualTo(2);
    }

    private static InetSocketAddress unusedLocalAddress() throws IOException {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket socket = new ServerSocket(0, 1, loopback)) {
            return new InetSocketAddress(loopback, socket.getLocalPort());
        }
    }
}
