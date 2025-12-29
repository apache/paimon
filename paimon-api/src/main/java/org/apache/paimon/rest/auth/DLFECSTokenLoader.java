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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.SimpleHttpClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;

/** DLF Token Loader for ECS Metadata Service. */
public class DLFECSTokenLoader implements DLFTokenLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DLFECSTokenLoader.class);

    private final String ecsMetadataURL;

    private String roleName;

    public DLFECSTokenLoader(String ecsMetaDataURL, @Nullable String roleName) {
        this.ecsMetadataURL = ecsMetaDataURL;
        this.roleName = roleName;
    }

    @Override
    public DLFToken loadToken() {
        if (roleName == null) {
            roleName = getRole(ecsMetadataURL);
        }
        return getToken(ecsMetadataURL + roleName);
    }

    @Override
    public String description() {
        return ecsMetadataURL;
    }

    private static String getRole(String url) {
        try {
            return getResponseBody(url);
        } catch (Exception e) {
            throw new RuntimeException("get role failed, error : " + e.getMessage(), e);
        }
    }

    private static DLFToken getToken(String url) {
        try {
            String token = SimpleHttpClient.INSTANCE.get(url);
            return RESTApi.fromJson(token, DLFToken.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new RuntimeException("get token failed, error : " + e.getMessage(), e);
        }
    }

    @VisibleForTesting
    protected static String getResponseBody(String url) {
        long startTime = System.currentTimeMillis();
        try {
            String responseBodyStr = SimpleHttpClient.INSTANCE.get(url);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "get response success, url : {}, cost : {} ms",
                        url,
                        System.currentTimeMillis() - startTime);
            }
            return responseBodyStr;
        } catch (Exception e) {
            throw new RuntimeException("get response failed, error : " + e.getMessage(), e);
        }
    }
}
