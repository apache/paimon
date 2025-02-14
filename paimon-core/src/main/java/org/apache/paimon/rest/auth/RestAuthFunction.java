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

import java.util.Map;
import java.util.function.Function;

/** The function used to generate auth header for the rest request. */
public class RestAuthFunction implements Function<RestAuthParameter, Map<String, String>> {

    private final Map<String, String> initHeader;
    private final AuthSession catalogAuth;

    public RestAuthFunction(Map<String, String> initHeader, AuthSession catalogAuth) {
        this.initHeader = initHeader;
        this.catalogAuth = catalogAuth;
    }

    @Override
    public Map<String, String> apply(RestAuthParameter restAuthParameter) {
        if (catalogAuth != null) {
            return catalogAuth.getAuthProvider().header(initHeader, restAuthParameter);
        }
        return initHeader;
    }
}
