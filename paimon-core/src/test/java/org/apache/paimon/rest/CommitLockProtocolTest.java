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

package org.apache.paimon.rest;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.requests.CommitLockRequest;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.responses.CommitLockResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the owner lease wire protocol without changing legacy snapshot requests. */
class CommitLockProtocolTest {
    @Test
    void acquireRenewAndCommitUseTheSameOwnerWithoutToken() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            Options options = new Options();
            options.set(RESTCatalogOptions.URI, server.url("/").toString());
            options.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");
            options.set(RESTCatalogOptions.TOKEN, "token");
            options.set(RESTCatalogInternalOptions.PREFIX, "catalog");
            RESTApi api = new RESTApi(options, false);
            Identifier identifier = Identifier.create("db", "table");
            CommitLockResponse grant =
                    new CommitLockResponse(true, "morax-job", 100000, 60000, snapshot(1));
            server.enqueue(json(RESTApi.toJson(grant)));
            CommitLockResponse acquired =
                    api.acquireCommitLock(identifier, "table-id", "morax-job");
            assertThat(acquired.isAcquired()).isTrue();
            assertThat(acquired.getSnapshot().id()).isEqualTo(1);
            RecordedRequest acquire = server.takeRequest(10, TimeUnit.SECONDS);
            assertThat(acquire.getPath())
                    .isEqualTo("/v1/catalog/databases/db/tables/table/commit-lock");
            CommitLockRequest request =
                    RESTApi.fromJson(acquire.getBody().readUtf8(), CommitLockRequest.class);
            assertThat(request.getTableId()).isEqualTo("table-id");
            assertThat(request.getCommitUser()).isEqualTo("morax-job");
            server.enqueue(json(RESTApi.toJson(grant)));
            assertThat(api.renewCommitLock(identifier, "table-id", "morax-job")).isTrue();
            assertThat(server.takeRequest(10, TimeUnit.SECONDS).getPath())
                    .endsWith("/commit-lock/renew");
            server.enqueue(json("{\"success\":true}"));
            assertThat(
                            api.commitSnapshot(
                                    identifier,
                                    "table-id",
                                    null,
                                    snapshot(2),
                                    Collections.emptyList()))
                    .isTrue();
            String commitBody = server.takeRequest(10, TimeUnit.SECONDS).getBody().readUtf8();
            ObjectNode payload = RESTApi.fromJson(commitBody, ObjectNode.class);
            assertThat(payload.has("lockToken")).isFalse();
            assertThat(payload.has("commitUser")).isFalse();
            assertThat(
                            RESTApi.fromJson(commitBody, CommitTableRequest.class)
                                    .getSnapshot()
                                    .commitUser())
                    .isEqualTo("morax-job");
            assertThat(RESTApi.fromJson(commitBody, CommitTableRequest.class).getTableId())
                    .isEqualTo("table-id");
        }
    }

    private static Snapshot snapshot(long id) {
        return new Snapshot(
                id,
                0L,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "morax-job",
                null,
                0L,
                Snapshot.CommitKind.APPEND,
                1000L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static MockResponse json(String body) {
        return new MockResponse()
                .setResponseCode(200)
                .setBody(body)
                .addHeader("Content-Type", "application/json");
    }
}
