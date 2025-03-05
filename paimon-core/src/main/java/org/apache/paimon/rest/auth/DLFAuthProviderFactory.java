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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.rest.RESTCatalogOptions.DLF_TOKEN_PATH;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_REFRESH_TIME;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;

/** Factory for {@link DLFAuthProvider}. */
public class DLFAuthProviderFactory implements AuthProviderFactory {

    @Override
    public String identifier() {
        return AuthProviderEnum.DLF.identifier();
    }

    @Override
    public AuthProvider create(Options options) {
        String region =
                options.getOptional(RESTCatalogOptions.DLF_REGION)
                        .orElseGet(() -> parseRegionFromUri(options.get(URI)));
        if (options.getOptional(RESTCatalogOptions.DLF_TOKEN_PATH).isPresent()) {
            String tokenFilePath = options.get(DLF_TOKEN_PATH);
            long tokenRefreshInMills = options.get(TOKEN_REFRESH_TIME).toMillis();
            return DLFAuthProvider.buildRefreshToken(tokenFilePath, tokenRefreshInMills, region);
        } else if (options.getOptional(RESTCatalogOptions.DLF_ACCESS_KEY_ID).isPresent()
                && options.getOptional(RESTCatalogOptions.DLF_ACCESS_KEY_SECRET).isPresent()) {
            return DLFAuthProvider.buildAKToken(
                    options.get(RESTCatalogOptions.DLF_ACCESS_KEY_ID),
                    options.get(RESTCatalogOptions.DLF_ACCESS_KEY_SECRET),
                    options.get(RESTCatalogOptions.DLF_SECURITY_TOKEN),
                    region);
        }
        throw new IllegalArgumentException("DLF token path or AK must be set for DLF Auth.");
    }

    protected static String parseRegionFromUri(String uri) {
        try {
            String regex = "dlf-(?:pre-)?([a-z]+-[a-z]+(?:-\\d+)?)";

            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(uri);

            if (matcher.find()) {
                return matcher.group(1);
            }
        } catch (Exception ignore) {
        }
        throw new IllegalArgumentException(
                "Could not get region from conf or uri, please check your config.");
    }
}
