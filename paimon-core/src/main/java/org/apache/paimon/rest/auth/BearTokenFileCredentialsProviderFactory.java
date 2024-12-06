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

import org.apache.paimon.options.Options;

/** factory for create {@link BearTokenCredentialsProvider}. */
public class BearTokenFileCredentialsProviderFactory implements CredentialsProviderFactory {

    @Override
    public String identifier() {
        return CredentialsProviderType.BEAR_TOKEN_FILE.name();
    }

    @Override
    public CredentialsProvider create(Options options) {
        boolean keepTokenRefreshed = options.get(AuthOptions.TOKEN_REFRESH_ENABLED);
        long tokenExpireInMills = options.get(AuthOptions.TOKEN_EXPIRES_IN).toMillis();
        String tokenFilePath = options.getOptional(AuthOptions.TOKEN_FILE_PATH).orElse(null);
        return new BearTokenFileCredentialsProvider(
                tokenFilePath, keepTokenRefreshed, -1L, tokenExpireInMills);
    }
}
