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

import java.util.HashMap;
import java.util.Map;

/** Auth provider for bear token. */
public class BearTokenAuthProvider implements AuthProvider {

    public static final String AUTHORIZATION_HEADER_KEY = "Authorization";

    private static final String BEARER_PREFIX = "Bearer ";

    protected String token;

    public BearTokenAuthProvider(String token) {
        this.token = token;
    }

    @Override
    public Map<String, String> header(
            Map<String, String> baseHeader, RESTAuthParameter restAuthParameter) {
        Map<String, String> headersWithAuth = new HashMap<>(baseHeader);
        headersWithAuth.put(AUTHORIZATION_HEADER_KEY, BEARER_PREFIX + token);
        return headersWithAuth;
    }

    @Override
    public boolean refresh() {
        return true;
    }
}
