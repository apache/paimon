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

import javax.annotation.Nullable;

/** Auth options. */
public class AuthConfig {
    private final @Nullable String token;
    private final Boolean keepRefreshed;
    private final @Nullable Long expiresAtMillis;
    private final @Nullable Long expiresInMills;

    public AuthConfig(
            @Nullable String token,
            boolean keepRefreshed,
            Long expiresAtMillis,
            Long expiresInMills) {
        this.token = token;
        this.keepRefreshed = keepRefreshed;
        this.expiresAtMillis = expiresAtMillis;
        this.expiresInMills = expiresInMills;
    }

    @Nullable
    public String token() {
        return token;
    }

    public boolean keepRefreshed() {
        return keepRefreshed;
    }

    public Long expiresAtMillis() {
        return expiresAtMillis;
    }

    public Long expiresInMills() {
        return expiresInMills;
    }
}
