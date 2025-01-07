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
import org.apache.paimon.rest.RESTCatalogOptions;

import static org.apache.paimon.rest.RESTCatalogInternalOptions.CREDENTIALS_PROVIDER;

/** Factory for creating {@link CredentialsProvider}. */
public interface CredentialsProviderFactory extends Factory {

    default CredentialsProvider create(Options options) {
        throw new UnsupportedOperationException(
                "Use  create(context) for " + this.getClass().getSimpleName());
    }

    static CredentialsProvider createCredentialsProvider(Options options, ClassLoader classLoader) {
        String credentialsProviderIdentifier = getCredentialsProviderTypeByConf(options).name();
        CredentialsProviderFactory credentialsProviderFactory =
                FactoryUtil.discoverFactory(
                        classLoader,
                        CredentialsProviderFactory.class,
                        credentialsProviderIdentifier);
        return credentialsProviderFactory.create(options);
    }

    static CredentialsProviderType getCredentialsProviderTypeByConf(Options options) {
        if (options.getOptional(CREDENTIALS_PROVIDER).isPresent()) {
            return CredentialsProviderType.valueOf(options.get(CREDENTIALS_PROVIDER));
        } else if (options.getOptional(RESTCatalogOptions.TOKEN_PROVIDER_PATH).isPresent()) {
            return CredentialsProviderType.BEAR_TOKEN_FILE;
        }
        return CredentialsProviderType.BEAR_TOKEN;
    }
}
