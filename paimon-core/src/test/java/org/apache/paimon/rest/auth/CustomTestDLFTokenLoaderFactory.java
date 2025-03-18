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

import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_ID;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_SECRET;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_SECURITY_TOKEN;

/** Factory for {@link CustomTestDLFTokenLoader}. */
public class CustomTestDLFTokenLoaderFactory implements DLFTokenLoaderFactory {

    @Override
    public String identifier() {
        return "custom";
    }

    @Override
    public DLFTokenLoader create(Options options) {
        String accessKeyId = options.get(DLF_ACCESS_KEY_ID);
        String accessKeySecret = options.get(DLF_ACCESS_KEY_SECRET);
        String securityToken = options.get(DLF_SECURITY_TOKEN);
        return new CustomTestDLFTokenLoader(accessKeyId, accessKeySecret, securityToken);
    }
}
