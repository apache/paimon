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

package org.apache.paimon.utils;

import okhttp3.Dispatcher;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;

import static okhttp3.ConnectionSpec.CLEARTEXT;
import static okhttp3.ConnectionSpec.COMPATIBLE_TLS;
import static okhttp3.ConnectionSpec.MODERN_TLS;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/** A simple client to wrap okhttp3. */
public class SimpleHttpClient implements Closeable {

    private static final MediaType MEDIA_TYPE = MediaType.parse("application/json");

    private final OkHttpClient client;

    public SimpleHttpClient(String threadName) {
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .dispatcher(
                                new Dispatcher(
                                        createCachedThreadPool(
                                                1, threadName, new SynchronousQueue<>())))
                        .retryOnConnectionFailure(true)
                        .connectionSpecs(Arrays.asList(MODERN_TLS, COMPATIBLE_TLS, CLEARTEXT))
                        .connectTimeout(Duration.ofMinutes(3))
                        .readTimeout(Duration.ofMinutes(3));

        this.client = builder.build();
    }

    public String post(String url, byte[] body, Map<String, String> headers) throws IOException {
        RequestBody requestBody = RequestBody.create(body, MEDIA_TYPE);
        Request request =
                new Request.Builder()
                        .url(url)
                        .post(requestBody)
                        .headers(Headers.of(headers))
                        .build();
        try (Response response = client.newCall(request).execute()) {
            String responseBodyStr = response.body() != null ? response.body().string() : null;
            if (!response.isSuccessful() || StringUtils.isNullOrWhitespaceOnly(responseBodyStr)) {
                throw new RuntimeException(
                        response.isSuccessful()
                                ? "ResponseBody is null or empty."
                                : String.format(
                                        "Response is not successful, response is %s", response));
            }
            return responseBodyStr;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            client.dispatcher().cancelAll();
            client.connectionPool().evictAll();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
