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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.rest.ExponentialHttpRetryInterceptor;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;

import static okhttp3.ConnectionSpec.CLEARTEXT;
import static okhttp3.ConnectionSpec.COMPATIBLE_TLS;
import static okhttp3.ConnectionSpec.MODERN_TLS;
import static org.apache.paimon.rest.RESTObjectMapper.OBJECT_MAPPER;

/** DLF Token Loader for ECS Metadata Service. */
public class DLFECSTokenLoader implements DLFTokenLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DLFECSTokenLoader.class);

    private static final OkHttpClient HTTP_CLIENT =
            new OkHttpClient.Builder()
                    .retryOnConnectionFailure(true)
                    .connectionSpecs(Arrays.asList(MODERN_TLS, COMPATIBLE_TLS, CLEARTEXT))
                    .addInterceptor(new ExponentialHttpRetryInterceptor(3))
                    .connectTimeout(Duration.ofMinutes(3))
                    .readTimeout(Duration.ofMinutes(3))
                    .build();

    private String ecsMetadataURL;

    private String roleName;

    public DLFECSTokenLoader(String ecsMetaDataURL, @Nullable String roleName) {
        this.ecsMetadataURL = ecsMetaDataURL;
        this.roleName = roleName;
    }

    @Override
    public DLFToken loadToken() {
        if (roleName == null) {
            roleName = getRole(ecsMetadataURL);
        }
        return getToken(ecsMetadataURL + roleName);
    }

    private static String getRole(String url) {
        try {
            return getResponseBody(url);
        } catch (Exception e) {
            throw new RuntimeException("get role failed, error : " + e.getMessage(), e);
        }
    }

    private static DLFToken getToken(String url) {
        try {
            String token = getResponseBody(url);
            return OBJECT_MAPPER.readValue(token, DLFToken.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new RuntimeException("get token failed, error : " + e.getMessage(), e);
        }
    }

    @VisibleForTesting
    protected static String getResponseBody(String url) {
        Request request = new Request.Builder().url(url).get().build();
        long startTime = System.currentTimeMillis();
        try (Response response = HTTP_CLIENT.newCall(request).execute()) {
            if (response == null) {
                throw new RuntimeException("get response failed, response is null");
            }
            if (!response.isSuccessful()) {
                throw new RuntimeException("get response failed, response : " + response);
            }
            String responseBodyStr = response.body() != null ? response.body().string() : null;
            if (responseBodyStr == null) {
                throw new RuntimeException("get response failed, response body is null");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "get response success, url : {}, cost : {} ms",
                        url,
                        System.currentTimeMillis() - startTime);
            }
            return responseBodyStr;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("get response failed, error : " + e.getMessage(), e);
        }
    }
}
