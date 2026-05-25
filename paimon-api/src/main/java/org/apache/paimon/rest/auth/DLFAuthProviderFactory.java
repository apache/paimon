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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.rest.RESTCatalogOptions.URI;

/** Factory for {@link DLFAuthProvider}. */
public class DLFAuthProviderFactory implements AuthProviderFactory {

    @Override
    public String identifier() {
        return AuthProviderEnum.DLF.identifier();
    }

    @Override
    public AuthProvider create(Options options) {
        String uri = options.get(URI);
        String region =
                options.getOptional(RESTCatalogOptions.DLF_REGION)
                        .orElseGet(() -> parseRegionFromUri(uri));
        String signingAlgorithm =
                options.getOptional(RESTCatalogOptions.DLF_SIGNING_ALGORITHM)
                        .orElseGet(() -> parseSigningAlgoFromUri(uri));

        if (options.getOptional(RESTCatalogOptions.DLF_TOKEN_LOADER).isPresent()) {
            DLFTokenLoader dlfTokenLoader =
                    DLFTokenLoaderFactory.createDLFTokenLoader(
                            options.get(RESTCatalogOptions.DLF_TOKEN_LOADER), options);
            return DLFAuthProvider.fromTokenLoader(dlfTokenLoader, uri, region, signingAlgorithm);
        } else if (options.getOptional(RESTCatalogOptions.DLF_TOKEN_PATH).isPresent()) {
            DLFTokenLoader dlfTokenLoader =
                    DLFTokenLoaderFactory.createDLFTokenLoader("local_file", options);
            return DLFAuthProvider.fromTokenLoader(dlfTokenLoader, uri, region, signingAlgorithm);
        } else if (options.getOptional(RESTCatalogOptions.DLF_ACCESS_KEY_ID).isPresent()
                && options.getOptional(RESTCatalogOptions.DLF_ACCESS_KEY_SECRET).isPresent()) {
            return DLFAuthProvider.fromAccessKey(
                    options.get(RESTCatalogOptions.DLF_ACCESS_KEY_ID),
                    options.get(RESTCatalogOptions.DLF_ACCESS_KEY_SECRET),
                    options.get(RESTCatalogOptions.DLF_SECURITY_TOKEN),
                    uri,
                    region,
                    signingAlgorithm);
        }
        throw new IllegalArgumentException("DLF token path or AK must be set for DLF Auth.");
    }

    protected static String parseRegionFromUri(String uri) {
        try {
            String regex = "(?:pre-)?([a-z]+-[a-z]+(?:-\\d+)?)";

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

    /**
     * Parse signing algorithm from uri. Automatically selects the appropriate signer based on the
     * endpoint uri.
     *
     * @param uri endpoint uri
     * @return signing algorithm identifier
     */
    protected static String parseSigningAlgoFromUri(String uri) {
        if (StringUtils.isEmpty(uri)) {
            return DLFDefaultSigner.IDENTIFIER;
        }

        // Check for aliyun openapi endpoints
        if (uri.toLowerCase().contains("dlfnext")) {
            return DLFOpenApiSigner.IDENTIFIER;
        }

        // Default to dlf for unknown hosts
        return DLFDefaultSigner.IDENTIFIER;
    }
}
