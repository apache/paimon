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

import static org.apache.paimon.rest.auth.AuthOptions.TOKEN_EXPIRES_IN;
import static org.apache.paimon.rest.auth.AuthOptions.TOKEN_FILE_PATH;

/** factory for create {@link BearTokenCredentialsProvider}. */
public class BearTokenFileCredentialsProviderFactory implements CredentialsProviderFactory {

    @Override
    public String identifier() {
        return CredentialsProviderType.BEAR_TOKEN_FILE.name();
    }

    @Override
    public CredentialsProvider create(Options options) {
        if (!options.getOptional(TOKEN_FILE_PATH).isPresent()) {
            throw new IllegalArgumentException(TOKEN_FILE_PATH.key() + " is required");
        }
        String tokenFilePath = options.get(TOKEN_FILE_PATH);
        boolean keepTokenRefreshed = options.get(AuthOptions.TOKEN_REFRESH_ENABLED);
        if (keepTokenRefreshed) {
            if (!options.getOptional(TOKEN_EXPIRES_IN).isPresent()) {
                throw new IllegalArgumentException(
                        TOKEN_EXPIRES_IN.key() + " is required when token refresh enabled");
            }
            long tokenExpireInMills = options.get(TOKEN_EXPIRES_IN).toMillis();
            return new BearTokenFileCredentialsProvider(tokenFilePath, tokenExpireInMills);

        } else {
            return new BearTokenFileCredentialsProvider(tokenFilePath);
        }
    }
}
