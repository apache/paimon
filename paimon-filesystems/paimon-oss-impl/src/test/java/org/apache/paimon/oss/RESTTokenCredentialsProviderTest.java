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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTTokenRefresher;

import com.aliyun.oss.OSSClient;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RESTTokenCredentialsProvider}. */
public class RESTTokenCredentialsProviderTest {

    private FakeRESTAndOSSServer server;

    @BeforeEach
    public void startServer() throws Exception {
        server = new FakeRESTAndOSSServer();
    }

    @AfterEach
    public void stopServer() {
        server.close();
    }

    @Test
    public void testUsesTheTokenInTheOptionsWhileItHasTimeLeft() {
        RESTTokenCredentialsProvider provider = provider(tableOptions("ak-1", Duration.ofHours(2)));

        assertThat(provider.getCredentials().getAccessKeyId()).isEqualTo("ak-1");
        assertThat(provider.getCredentials().getSecurityToken()).isEqualTo("token-ak-1");
        assertThat(server.tokenRequests()).isZero();
    }

    @Test
    public void testLoadsANewTokenFromTheCatalogWhenTheCurrentOneIsAboutToExpire() {
        server.addToken("ak-2", System.currentTimeMillis() + Duration.ofHours(4).toMillis());
        RESTTokenCredentialsProvider provider =
                provider(tableOptions("ak-1", Duration.ofMinutes(30)));

        assertThat(provider.getCredentials().getAccessKeyId()).isEqualTo("ak-2");
        int requests = server.tokenRequests();
        assertThat(provider.getCredentials().getAccessKeyId()).isEqualTo("ak-2");
        assertThat(server.tokenRequests()).isEqualTo(requests);
    }

    @Test
    public void testOSSFileIOInstallsTheProviderOnlyForARESTTable() throws Exception {
        OSSFileIO restFileIO = new OSSFileIO();
        OSSFileIO plainFileIO = new OSSFileIO();
        try {
            restFileIO.configure(CatalogContext.create(tableOptions("ak-1", Duration.ofHours(2))));
            OSSClient client = restFileIO.ossClient(new Path("oss://bucket/key"));
            assertThat(client.getCredentialsProvider())
                    .isInstanceOf(RESTTokenCredentialsProvider.class);
            assertThat(client.getCredentialsProvider().getCredentials().getAccessKeyId())
                    .isEqualTo("ak-1");
            // readers that build their own clients from these options keep the static keys
            assertThat(restFileIO.hadoopOptions().keySet())
                    .doesNotContain("fs.oss.credentials.provider")
                    .noneMatch(key -> key.startsWith("fs.oss.rest-token."));

            Options plain = new Options();
            plain.set("fs.oss.endpoint", server.endpoint());
            plain.set("fs.oss.accessKeyId", "ak-1");
            plain.set("fs.oss.accessKeySecret", "secret-ak-1");
            plainFileIO.configure(CatalogContext.create(plain));
            assertThat(plainFileIO.ossClient(new Path("oss://bucket/key")).getCredentialsProvider())
                    .isNotInstanceOf(RESTTokenCredentialsProvider.class);
        } finally {
            restFileIO.close();
            plainFileIO.close();
        }
    }

    /** Catalog options with a merged token, as RESTTokenFileIO hands them to its delegate. */
    private Options tableOptions(String accessKeyId, Duration lifetime) {
        Options options = server.catalogOptions();
        server.ossToken(accessKeyId).forEach(options::set);
        options.set("file-io.allow-cache", "false");
        RESTTokenRefresher.configure(
                options,
                Identifier.create("db", "table"),
                System.currentTimeMillis() + lifetime.toMillis());
        return options;
    }

    private static RESTTokenCredentialsProvider provider(Options options) {
        Configuration conf = new Configuration(false);
        options.toMap()
                .forEach(
                        (key, value) ->
                                conf.set(
                                        RESTTokenCredentialsProvider.CATALOG_OPTIONS_PREFIX + key,
                                        value));
        return new RESTTokenCredentialsProvider(null, conf);
    }
}
