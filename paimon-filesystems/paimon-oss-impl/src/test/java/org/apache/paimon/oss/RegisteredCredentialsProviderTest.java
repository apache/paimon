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
import org.apache.paimon.fs.CredentialsSupplierRegistry;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.comm.ExecutionContext;
import com.aliyun.oss.common.comm.RequestMessage;
import com.aliyun.oss.common.comm.ResponseMessage;
import com.aliyun.oss.common.comm.RetryStrategy;
import com.aliyun.oss.common.comm.ServiceClient;
import com.aliyun.oss.internal.OSSObjectOperation;
import com.aliyun.oss.model.GenericRequest;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RegisteredCredentialsProvider}. */
public class RegisteredCredentialsProviderTest {

    @Test
    public void testEveryRequestIsSignedWithTheCurrentCredentials() throws Exception {
        AtomicReference<String> accessKeyId = new AtomicReference<>("ak-1");
        Supplier<Map<String, String>> supplier = () -> credentials(accessKeyId.get());
        String id = CredentialsSupplierRegistry.register(supplier);
        List<String> authorizations = new ArrayList<>();
        OSSObjectOperation operation =
                new OSSObjectOperation(capturingClient(authorizations), provider(id, null));
        operation.setEndpoint(new URI("http://oss-cn-hangzhou.aliyuncs.com"));

        GenericRequest request = new GenericRequest("bucket", "key");
        assertThatThrownBy(() -> operation.getObjectMetadata(request))
                .isInstanceOf(ClientException.class);
        accessKeyId.set("ak-2");
        assertThatThrownBy(() -> operation.getObjectMetadata(request))
                .isInstanceOf(ClientException.class);

        assertThat(authorizations).hasSize(2);
        assertThat(authorizations.get(0)).contains("ak-1").doesNotContain("ak-2");
        assertThat(authorizations.get(1)).contains("ak-2").doesNotContain("ak-1");
    }

    @Test
    public void testUsesConfiguredCredentialsWithoutARegisteredSupplier() {
        RegisteredCredentialsProvider provider = provider("unknown-id", "ak-1");
        assertThat(provider.getCredentials().getAccessKeyId()).isEqualTo("ak-1");
    }

    @Test
    public void testKeepsTheLastCredentialsWhenTheSupplierFails() {
        AtomicBoolean failing = new AtomicBoolean(false);
        Supplier<Map<String, String>> supplier =
                () -> {
                    if (failing.get()) {
                        throw new IllegalStateException("REST server unavailable");
                    }
                    return credentials("ak-2");
                };
        String id = CredentialsSupplierRegistry.register(supplier);
        RegisteredCredentialsProvider provider = provider(id, null);
        assertThat(provider.getCredentials().getAccessKeyId()).isEqualTo("ak-2");

        failing.set(true);
        assertThat(provider.getCredentials().getAccessKeyId()).isEqualTo("ak-2");
        assertThatThrownBy(() -> provider(id, null).getCredentials())
                .hasMessageContaining("REST server unavailable");
        assertThat(supplier).isNotNull();
    }

    @Test
    public void testOSSFileIOResolvesCredentialsFromTheRegisteredSupplier() throws Exception {
        Supplier<Map<String, String>> supplier = () -> credentials("ak-2");
        String id = CredentialsSupplierRegistry.register(supplier);
        OSSFileIO fileIO = new OSSFileIO();
        try {
            Options options = new Options();
            options.set("fs.oss.endpoint", "http://oss-cn-hangzhou.aliyuncs.com");
            options.set("fs.oss.accessKeyId", "ak-1");
            options.set("fs.oss.accessKeySecret", "sk-1");
            options.set(CredentialsSupplierRegistry.SUPPLIER_ID, id);
            fileIO.configure(CatalogContext.create(options));

            OSSClient client = fileIO.ossClient(new Path("oss://bucket/key"));
            assertThat(client.getCredentialsProvider())
                    .isInstanceOf(RegisteredCredentialsProvider.class);
            assertThat(client.getCredentialsProvider().getCredentials().getAccessKeyId())
                    .isEqualTo("ak-2");
            // readers that build their own clients from these options keep the static keys
            assertThat(fileIO.hadoopOptions().keySet())
                    .doesNotContain("fs.oss.credentials.provider");
        } finally {
            fileIO.close();
        }
    }

    private static Map<String, String> credentials(String accessKeyId) {
        Map<String, String> credentials = new HashMap<>();
        credentials.put("fs.oss.accessKeyId", accessKeyId);
        credentials.put("fs.oss.accessKeySecret", "secret-" + accessKeyId);
        credentials.put("fs.oss.securityToken", "token-" + accessKeyId);
        return credentials;
    }

    private static RegisteredCredentialsProvider provider(String id, String accessKeyId) {
        Configuration conf = new Configuration(false);
        conf.set(CredentialsSupplierRegistry.SUPPLIER_ID, id);
        if (accessKeyId != null) {
            conf.set("fs.oss.accessKeyId", accessKeyId);
            conf.set("fs.oss.accessKeySecret", "secret-" + accessKeyId);
        }
        return new RegisteredCredentialsProvider(null, conf);
    }

    /** Records the Authorization header of each request instead of sending it. */
    private static ServiceClient capturingClient(List<String> authorizations) {
        return new ServiceClient(new ClientConfiguration()) {
            @Override
            protected ResponseMessage sendRequestCore(
                    ServiceClient.Request request, ExecutionContext context) {
                authorizations.add(request.getHeaders().get("Authorization"));
                throw new ClientException("captured");
            }

            @Override
            protected RetryStrategy getDefaultRetryStrategy() {
                return new RetryStrategy() {
                    @Override
                    public boolean shouldRetry(
                            Exception e,
                            RequestMessage request,
                            ResponseMessage response,
                            int retries) {
                        return false;
                    }
                };
            }

            @Override
            public void shutdown() {}
        };
    }
}
