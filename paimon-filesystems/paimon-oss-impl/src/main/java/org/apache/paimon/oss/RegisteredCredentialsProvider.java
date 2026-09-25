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

package org.apache.paimon.oss;

import org.apache.paimon.fs.CredentialsSupplierRegistry;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/**
 * An OSS {@link CredentialsProvider} that asks a supplier from {@link CredentialsSupplierRegistry}
 * for credentials on every request, so streams that are already open also pick up refreshed ones.
 * It holds the supplier for as long as the OSS client that uses it is alive.
 */
public class RegisteredCredentialsProvider implements CredentialsProvider {

    private static final Logger LOG = LoggerFactory.getLogger(RegisteredCredentialsProvider.class);

    static final String ACCESS_KEY_ID = "fs.oss.accessKeyId";
    static final String ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    static final String SECURITY_TOKEN = "fs.oss.securityToken";

    private final String supplierId;
    @Nullable private final Supplier<Map<String, String>> supplier;

    // The last options from the supplier and the credentials built from them, swapped together.
    private volatile Resolved last;

    public RegisteredCredentialsProvider(URI uri, Configuration conf) {
        this.supplierId = conf.get(CredentialsSupplierRegistry.SUPPLIER_ID);
        this.supplier = supplierId == null ? null : CredentialsSupplierRegistry.get(supplierId);
        String accessKeyId = conf.get(ACCESS_KEY_ID);
        String accessKeySecret = conf.get(ACCESS_KEY_SECRET);
        if (accessKeyId != null && accessKeySecret != null) {
            this.last =
                    new Resolved(
                            null,
                            new DefaultCredentials(
                                    accessKeyId, accessKeySecret, conf.get(SECURITY_TOKEN)));
        }
    }

    @Override
    public void setCredentials(Credentials credentials) {
        this.last = new Resolved(null, credentials);
    }

    @Override
    public Credentials getCredentials() {
        Resolved resolved = last;
        if (supplier != null) {
            try {
                return resolve(supplier.get(), resolved);
            } catch (RuntimeException e) {
                if (resolved == null) {
                    throw e;
                }
                LOG.warn("Failed to refresh OSS credentials, reusing the last ones.", e);
            }
        }
        if (resolved == null) {
            throw new IllegalStateException(
                    "No OSS credentials available for credentials supplier " + supplierId);
        }
        return resolved.credentials;
    }

    private Credentials resolve(Map<String, String> options, Resolved resolved) {
        if (resolved != null && resolved.options == options) {
            return resolved.credentials;
        }
        String accessKeyId = null;
        String accessKeySecret = null;
        String securityToken = null;
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (key.equals(ACCESS_KEY_ID.toLowerCase(Locale.ROOT))) {
                accessKeyId = entry.getValue();
            } else if (key.equals(ACCESS_KEY_SECRET.toLowerCase(Locale.ROOT))) {
                accessKeySecret = entry.getValue();
            } else if (key.equals(SECURITY_TOKEN.toLowerCase(Locale.ROOT))) {
                securityToken = entry.getValue();
            }
        }
        if (accessKeyId == null || accessKeySecret == null) {
            throw new IllegalStateException(
                    "Credentials supplier " + supplierId + " returned no OSS access key.");
        }
        Credentials credentials =
                new DefaultCredentials(accessKeyId, accessKeySecret, securityToken);
        last = new Resolved(options, credentials);
        return credentials;
    }

    private static final class Resolved {

        private final Map<String, String> options;
        private final Credentials credentials;

        private Resolved(Map<String, String> options, Credentials credentials) {
            this.options = options;
            this.credentials = credentials;
        }
    }
}
