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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.rest.RESTObjectMapper.OBJECT_MAPPER;

/** Mock a http web service locally. */
public class TestHttpWebServer {

    private MockWebServer mockWebServer;
    private String baseUrl;
    private final String path;

    public TestHttpWebServer(String path) {
        this.path = path;
    }

    public void start() throws Exception {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        baseUrl = mockWebServer.url(path).toString();
    }

    public void stop() throws IOException {
        mockWebServer.shutdown();
    }

    public RecordedRequest takeRequest(long timeout, TimeUnit unit) throws Exception {
        return mockWebServer.takeRequest(timeout, unit);
    }

    public void enqueueResponse(String body, Integer code) {
        MockResponse mockResponseObj = generateMockResponse(body, code);
        enqueueResponse(mockResponseObj);
    }

    public void enqueueResponse(MockResponse mockResponseObj) {
        mockWebServer.enqueue(mockResponseObj);
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public MockResponse generateMockResponse(String data, Integer code) {
        return new MockResponse()
                .setResponseCode(code)
                .setBody(data)
                .addHeader("Content-Type", "application/json");
    }

    public String createResponseBody(RESTResponse response) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(response);
    }

    public <T> T readRequestBody(String body, Class<T> requestType) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(body, requestType);
    }
}
