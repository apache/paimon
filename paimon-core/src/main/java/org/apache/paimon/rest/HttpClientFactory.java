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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static okhttp3.ConnectionSpec.CLEARTEXT;
import static okhttp3.ConnectionSpec.COMPATIBLE_TLS;
import static okhttp3.ConnectionSpec.MODERN_TLS;

public class HttpClientFactory {

    public static OkHttpClient createOkHttpClient(
            int threadPoolSize, long connectTimeoutMillis, long readTimeoutMillis) {
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
                        .connectionSpecs(Arrays.asList(MODERN_TLS, COMPATIBLE_TLS, CLEARTEXT));

        return builder.build();
    }
}
