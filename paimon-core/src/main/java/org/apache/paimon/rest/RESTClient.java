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

import org.apache.paimon.rest.auth.RestAuthParameter;

import java.io.Closeable;
import java.util.Map;
import java.util.function.Function;

/** Interface for a basic HTTP Client for interfacing with the REST catalog. */
public interface RESTClient extends Closeable {

    <T extends RESTResponse> T get(
            String path,
            Class<T> responseType,
            Function<RestAuthParameter, Map<String, String>> headersFunction);

    <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Function<RestAuthParameter, Map<String, String>> headersFunction);

    <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            Function<RestAuthParameter, Map<String, String>> headersFunction);

    <T extends RESTResponse> T delete(
            String path, Function<RestAuthParameter, Map<String, String>> headersFunction);

    <T extends RESTResponse> T delete(
            String path,
            RESTRequest body,
            Function<RestAuthParameter, Map<String, String>> headersFunction);
}
