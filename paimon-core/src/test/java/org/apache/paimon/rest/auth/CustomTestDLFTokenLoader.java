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

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.apache.paimon.rest.auth.DLFAuthProvider.TOKEN_DATE_FORMATTER;

/** DLF Token Loader for custom. */
public class CustomTestDLFTokenLoader implements DLFTokenLoader {

    DLFToken token;

    public CustomTestDLFTokenLoader(
            String accessKeyId, String accessKeySecret, String securityToken) {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String expiration = now.format(TOKEN_DATE_FORMATTER);
        this.token = new DLFToken(accessKeyId, accessKeySecret, securityToken, expiration);
    }

    @Override
    public DLFToken loadToken() {
        return token;
    }
}
