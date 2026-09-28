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

import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenRefresher;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.Locale;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An OSS {@link CredentialsProvider} that refreshes the data token of a REST catalog table by
 * itself and is asked on every request, so streams that are already open also pick up refreshed
 * tokens.
 */
public class RESTTokenCredentialsProvider implements CredentialsProvider {

    /** Prefix under which {@link OSSFileIO} passes the catalog options to this provider. */
    static final String CATALOG_OPTIONS_PREFIX = "fs.oss.rest-token.catalog.";

    private static final String ACCESS_KEY_ID = "fs.oss.accessKeyId";
    private static final String ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    private static final String SECURITY_TOKEN = "fs.oss.securityToken";

    private final RESTTokenRefresher refresher;

    // The last token and the credentials built from it, swapped together.
    private volatile Resolved last;

    public RESTTokenCredentialsProvider(URI uri, Configuration conf) {
        Options options = new Options(conf.getPropsWithPrefix(CATALOG_OPTIONS_PREFIX));
        checkArgument(
                RESTTokenRefresher.isConfigured(options),
                "No REST catalog table configured under '%s'.",
                CATALOG_OPTIONS_PREFIX);
        this.refresher = RESTTokenRefresher.fromOptions(options);
    }

    @Override
    public void setCredentials(Credentials credentials) {
        throw new UnsupportedOperationException(
                "Credentials come from the REST catalog and cannot be set.");
    }

    @Override
    public Credentials getCredentials() {
        RESTToken token = refresher.token();
        Resolved resolved = last;
        if (resolved != null && resolved.token == token) {
            return resolved.credentials;
        }
        Credentials credentials = toCredentials(token.token());
        last = new Resolved(token, credentials);
        return credentials;
    }

    private static Credentials toCredentials(Map<String, String> token) {
        String accessKeyId = null;
        String accessKeySecret = null;
        String securityToken = null;
        for (Map.Entry<String, String> entry : token.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (key.equals(ACCESS_KEY_ID.toLowerCase(Locale.ROOT))) {
                accessKeyId = entry.getValue();
            } else if (key.equals(ACCESS_KEY_SECRET.toLowerCase(Locale.ROOT))) {
                accessKeySecret = entry.getValue();
            } else if (key.equals(SECURITY_TOKEN.toLowerCase(Locale.ROOT))) {
                securityToken = entry.getValue();
            }
        }
        checkArgument(
                accessKeyId != null && accessKeySecret != null,
                "The data token from the REST catalog has no OSS access key.");
        return new DefaultCredentials(accessKeyId, accessKeySecret, securityToken);
    }

    private static final class Resolved {

        private final RESTToken token;
        private final Credentials credentials;

        private Resolved(RESTToken token, Credentials credentials) {
            this.token = token;
            this.credentials = credentials;
        }
    }
}
