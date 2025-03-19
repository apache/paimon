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

package org.apache.paimon.rest.auth;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.io.IOException;

/** Mock ECS metadata service for testing. */
public class MockECSMetadataService {

    private static final ObjectMapper OBJECT_MAPPER_INSTANCE = new ObjectMapper();

    private final MockWebServer server;

    private DLFToken mockToken;

    public MockECSMetadataService(String ecsRoleName) {
        server = new MockWebServer();
        server.setDispatcher(initDispatcher(ecsRoleName));
    }

    public void start() throws IOException {
        server.start();
    }

    public void shutdown() throws IOException {
        server.shutdown();
    }

    public String getUrl() {
        return server.url("").toString();
    }

    public Dispatcher initDispatcher(String ecsRoleName) {
        return new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                String path = request.getPath();
                if (path.equals("/latest/meta-data/Ram/security-credentials/")) {
                    return new MockResponse().setResponseCode(200).setBody(ecsRoleName);
                } else if (path.equals(
                        "/latest/meta-data/Ram/security-credentials/" + ecsRoleName)) {
                    try {
                        String body = OBJECT_MAPPER_INSTANCE.writeValueAsString(mockToken);
                        return new MockResponse().setResponseCode(200).setBody(body);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    return new MockResponse().setResponseCode(404);
                }
            }
        };
    }

    public void setMockToken(DLFToken mockToken) {
        this.mockToken = mockToken;
    }

    public DLFToken getMockToken() {
        return mockToken;
    }
}
