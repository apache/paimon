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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.requests.RollbackTableRequest;
import org.apache.paimon.table.Instant;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests rollback routing and optional writer context through a delegated REST catalog. */
class RollbackProtocolTest {
    @ParameterizedTest
    @CsvSource({"true,snapshot", "false,snapshot", "true,tag", "false,tag"})
    void testRollbackCarriesWriterContextAndPreservesUnownedPayload(boolean owned, String kind)
            throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            Options options = new Options();
            options.set(RESTCatalogOptions.URI, server.url("/").toString());
            options.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");
            options.set(RESTCatalogOptions.TOKEN, "token");
            options.set(RESTCatalogInternalOptions.PREFIX, "catalog");
            try (Catalog catalog =
                    new DelegateCatalog(new RESTCatalog(CatalogContext.create(options), false)) {
                        @Override
                        public CatalogLoader catalogLoader() {
                            return wrapped.catalogLoader();
                        }
                    }) {
                Identifier identifier = new Identifier("db", "table", "dev");
                Instant instant =
                        "snapshot".equals(kind) ? Instant.snapshot(1L) : Instant.tag("tag-1");
                server.enqueue(
                        new MockResponse()
                                .setResponseCode(200)
                                .setHeader("Content-Type", "application/json")
                                .setBody("{}"));
                catalog.rollbackTo(
                        identifier,
                        owned ? "table-id" : null,
                        instant,
                        2L,
                        owned ? "Writer-1" : null);
                RecordedRequest recorded = server.takeRequest(10, TimeUnit.SECONDS);
                assertThat(recorded.getMethod()).isEqualTo("POST");
                assertThat(recorded.getPath())
                        .isEqualTo("/v1/catalog/databases/db/tables/table%24branch_dev/rollback");
                String body = recorded.getBody().readUtf8();
                ObjectNode payload = RESTApi.fromJson(body, ObjectNode.class);
                assertThat(payload.get("instant").get("type").asText()).isEqualTo(kind);
                RollbackTableRequest request = RESTApi.fromJson(body, RollbackTableRequest.class);
                assertThat(request.getFromSnapshot()).isEqualTo(2L);
                if ("snapshot".equals(kind)) {
                    assertThat(((Instant.SnapshotInstant) request.getInstant()).getSnapshotId())
                            .isEqualTo(1L);
                } else {
                    assertThat(((Instant.TagInstant) request.getInstant()).getTagName())
                            .isEqualTo("tag-1");
                }
                if (owned) {
                    assertThat(request.getTableId()).isEqualTo("table-id");
                    assertThat(request.getCommitUser()).isEqualTo("Writer-1");
                    assertThat(payload.size()).isEqualTo(4);
                } else {
                    assertThat(payload.has("tableId")).isFalse();
                    assertThat(payload.has("commitUser")).isFalse();
                    assertThat(payload.size()).isEqualTo(2);
                }
            }
        }
    }
}
