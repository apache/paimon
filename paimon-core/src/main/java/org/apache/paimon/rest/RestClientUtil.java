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

import org.apache.paimon.rest.responses.ErrorResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;

/** REST client util. */
public class RestClientUtil {
    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(RestClientUtil.class);
    private static final ObjectMapper mapper = null;

    public static <T> T getResponse(Call<T> call) {
        Request request = call.request();
        if (log.isDebugEnabled()) {
            log.debug("Begin: http call [{}]", request);
        }

        Response<T> response = getResponseUtil(call);
        T body = response.body();
        if (log.isDebugEnabled()) {
            log.debug(
                    "Http call success [{}], http code [{}]",
                    response.isSuccessful(),
                    response.code());
        }

        if (response.code() == 403) {
            throw new UnsupportedOperationException(
                    "This call is Forbidden, as cluster name is diff with call cluster.");
        }
        if (!response.isSuccessful() || body == null) {
            try (ResponseBody errorBody = response.errorBody()) {
                HttpUrl url = request.url();
                if (errorBody != null) {
                    String errorBodyStr = null;
                    try {
                        errorBodyStr = errorBody.string();
                        ErrorResponse errorResponse =
                                mapper.readValue(errorBody.toString(), ErrorResponse.class);
                        if (errorResponse != null
                                && errorResponse.getErrors() != null
                                && errorResponse.getErrors().size() > 0) {
                            errorBodyStr = String.join(",", errorResponse.getErrors());
                        }
                        log.error("Http call Error body: [{}]", errorBodyStr);
                    } catch (Throwable e) {
                        String logErrorBodyStr = errorBodyStr != null ? errorBodyStr : "empty";
                        log.warn(
                                "Http call parse errorBody [{}] from response failed:",
                                logErrorBodyStr,
                                e);
                    }

                    if (errorBodyStr != null) {
                        throw new IllegalStateException(
                                String.format(
                                        "Missing body in response for call %s. Error body: %s",
                                        url, errorBodyStr));
                    }
                    log.warn("Http call [{}] get errorBodyStr from response is empty", url);
                }
                log.error("Http call [{}] errorBody is empty", url);
                throw new IllegalStateException(
                        String.format("Missing body in response for call %s", url));
            }
        }
        return body;
    }

    private static <T> Response<T> getResponseUtil(Call<T> call) {
        long start = System.currentTimeMillis();
        try {
            return call.execute();
        } catch (IOException e) {
            throw new RuntimeException("Failed to execute call " + call.request().url(), e);
        } finally {
            long cost = System.currentTimeMillis() - start;

            if (log.isDebugEnabled()) {
                log.debug("{} {}, cost:{}ms", call.request().method(), call.request().url(), cost);
            }
        }
    }
}
