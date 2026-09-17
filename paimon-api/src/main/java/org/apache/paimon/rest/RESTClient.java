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

import org.apache.paimon.rest.auth.RESTAuthFunction;

import java.util.Map;

/** Interface for a basic HTTP Client for interfacing with the REST catalog. */
public interface RESTClient {

    <T extends RESTResponse> T get(
            String path, Class<T> responseType, RESTAuthFunction restAuthFunction);

    <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            RESTAuthFunction restAuthFunction);

    /** Sends a GET for the API that {@code requestType} names in its {@code API_NAME}. */
    default <T extends RESTResponse> T get(
            String path,
            Class<? extends RESTRequest> requestType,
            Class<T> responseType,
            RESTAuthFunction restAuthFunction) {
        return get(path, responseType, restAuthFunction);
    }

    /** Sends a GET with query parameters for the API that {@code requestType} names. */
    default <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<? extends RESTRequest> requestType,
            Class<T> responseType,
            RESTAuthFunction restAuthFunction) {
        return get(path, queryParams, responseType, restAuthFunction);
    }

    <T extends RESTResponse> T post(
            String path, RESTRequest body, RESTAuthFunction restAuthFunction);

    <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            RESTAuthFunction restAuthFunction);

    <T extends RESTResponse> T delete(String path, RESTAuthFunction restAuthFunction);

    /** Sends a DELETE for the API that {@code requestType} names in its {@code API_NAME}. */
    default <T extends RESTResponse> T delete(
            String path,
            Class<? extends RESTRequest> requestType,
            RESTAuthFunction restAuthFunction) {
        return delete(path, restAuthFunction);
    }

    <T extends RESTResponse> T delete(
            String path, RESTRequest body, RESTAuthFunction restAuthFunction);
}
