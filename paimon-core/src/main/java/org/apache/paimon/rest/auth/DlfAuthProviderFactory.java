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
import org.apache.paimon.rest.RESTCatalogOptions;

import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER_FILE_NAME;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER_PATH;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_REFRESH_TIME;

/** Factory for {@link DlfAuthProvider}. */
public class DlfAuthProviderFactory implements AuthProviderFactory {
    @Override
    public String identifier() {
        return AuthProviderEnum.DLF.identifier();
    }

    @Override
    public AuthProvider create(Options options) {
        if (options.getOptional(RESTCatalogOptions.TOKEN_PROVIDER_PATH).isPresent()) {
            if (!options.getOptional(TOKEN_PROVIDER_PATH).isPresent()) {
                throw new IllegalArgumentException(TOKEN_PROVIDER_PATH.key() + " is required");
            }
            String tokenFilePath = options.get(TOKEN_PROVIDER_PATH);
            long tokenRefreshInMills = options.get(TOKEN_REFRESH_TIME).toMillis();
            if (options.getOptional(TOKEN_REFRESH_TIME).isPresent()) {
                String tokenFileName = options.get(TOKEN_PROVIDER_FILE_NAME);
                return DlfAuthProvider.buildRefreshToken(
                        tokenFilePath, tokenFileName, tokenRefreshInMills);
            }
        }
        return DlfAuthProvider.buildAKToken(
                options.get(RESTCatalogOptions.TOKEN_ACCESS_KEY_ID),
                options.get(RESTCatalogOptions.TOKEN_ACCESS_KEY_SECRET));
    }
}
