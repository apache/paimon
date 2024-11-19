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

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static okhttp3.ConnectionSpec.CLEARTEXT;
import static okhttp3.ConnectionSpec.COMPATIBLE_TLS;
import static okhttp3.ConnectionSpec.MODERN_TLS;

/** HTTP client for REST catalog. */
public class HttpClient implements RESTClient {

    private final OkHttpClient okHttpClient;
    private final String endpoint;
    private final ObjectMapper mapper = RESTObjectMapper.create();

    public HttpClient(String endpoint, String initToken) {
        // todo: support config
        this.okHttpClient = createHttpClient(1, 3_000, 3_000, initToken);
        this.endpoint = endpoint;
    }

    public HttpClient(OkHttpClient okHttpClient, String endpoint) {
        this.okHttpClient = okHttpClient;
        this.endpoint = endpoint;
    }

    @Override
    public RESTCatalogApi getClient() {
        return createAgentCallRetrofit(okHttpClient, endpoint).create(RESTCatalogApi.class);
    }

    private Retrofit createAgentCallRetrofit(OkHttpClient httpClient, String baseUrl) {
        return new Retrofit.Builder()
                .client(requireNonNull(httpClient, "httpClient"))
                .baseUrl(requireNonNull(baseUrl, "baseUrl").toString())
                .addConverterFactory(JacksonConverterFactory.create())
                .validateEagerly(true)
                .build();
    }

    private static OkHttpClient createHttpClient(
            int threadPoolSize,
            long connectTimeoutMillis,
            long readTimeoutMillis,
            String initToken) {
        ExecutorService executorService =
                new ThreadPoolExecutor(
                        threadPoolSize,
                        threadPoolSize,
                        60,
                        TimeUnit.SECONDS,
                        new SynchronousQueue<>(),
                        new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat("rest catalog http client %d")
                                .build());

        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .connectTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS)
                        .readTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS)
                        .dispatcher(new Dispatcher(executorService))
                        .retryOnConnectionFailure(true)
                        .addInterceptor(new AuthenticationInterceptor(initToken))
                        .connectionSpecs(Arrays.asList(MODERN_TLS, COMPATIBLE_TLS, CLEARTEXT));

        return builder.build();
    }
}
