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
import org.apache.paimon.utils.StringUtils;

import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_EXPIRATION_TIME;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER_PATH;

/** Authentication provider. */
public interface AuthProvider {

    Map<String, String> authHeader();

    boolean refresh();

    default boolean supportRefresh() {
        return false;
    }

    default boolean keepRefreshed() {
        return false;
    }

    default boolean willSoonExpire() {
        return false;
    }

    default Optional<Long> expiresAtMillis() {
        return Optional.empty();
    }

    default Optional<Long> expiresInMills() {
        return Optional.empty();
    }

    static AuthProvider create(Options options) {
        if (options.getOptional(RESTCatalogOptions.TOKEN_PROVIDER_PATH).isPresent()) {
            if (!options.getOptional(TOKEN_PROVIDER_PATH).isPresent()) {
                throw new IllegalArgumentException(TOKEN_PROVIDER_PATH.key() + " is required");
            }
            String tokenFilePath = options.get(TOKEN_PROVIDER_PATH);
            if (options.getOptional(TOKEN_EXPIRATION_TIME).isPresent()) {
                long tokenExpireInMills = options.get(TOKEN_EXPIRATION_TIME).toMillis();
                return new BearTokenFileAuthProvider(tokenFilePath, tokenExpireInMills);

            } else {
                return new BearTokenFileAuthProvider(tokenFilePath);
            }
        } else {
            if (options.getOptional(RESTCatalogOptions.TOKEN)
                    .map(StringUtils::isNullOrWhitespaceOnly)
                    .orElse(true)) {
                throw new IllegalArgumentException(
                        RESTCatalogOptions.TOKEN.key() + " is required and not empty");
            }
            return new BearTokenAuthProvider(options.get(RESTCatalogOptions.TOKEN));
        }
    }
}
