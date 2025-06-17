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

import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;

/** Factory for {@link AuthProvider}. */
public interface AuthProviderFactory extends Factory {

    AuthProvider create(Options options);

    static AuthProvider createAuthProvider(Options options) {
        String tokenProvider = options.get(TOKEN_PROVIDER);
        if (StringUtils.isEmpty(tokenProvider)) {
            throw new IllegalArgumentException("token.provider is not set.");
        }
        AuthProviderFactory factory =
                FactoryUtil.discoverFactory(
                        AuthProviderFactory.class.getClassLoader(),
                        AuthProviderFactory.class,
                        tokenProvider);
        return factory.create(options);
    }
}
