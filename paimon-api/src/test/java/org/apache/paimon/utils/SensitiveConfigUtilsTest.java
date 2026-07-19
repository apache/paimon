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

package org.apache.paimon.utils;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.utils.SensitiveConfigUtils.REDACTED;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SensitiveConfigUtils}. */
class SensitiveConfigUtilsTest {

    @Test
    void testIsSensitiveDetectsCredentialKeys() {
        // Access keys / secrets in various vendor spellings.
        assertThat(SensitiveConfigUtils.isSensitive("fs.oss.accessKeyId")).isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("fs.oss.accessKeySecret")).isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("fs.s3a.access.key")).isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("fs.s3a.secret.key")).isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("fs.s3a.session.token")).isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("security.token")).isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("my.password")).isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("some.credential")).isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("Authorization")).isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("client.private-key")).isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("dlf.api-key")).isTrue();
        // Azure / S3 cloud credentials.
        assertThat(SensitiveConfigUtils.isSensitive("fs.azure.account.key.acct.blob.core"))
                .isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("fs.azure.sas.container.acct.blob.core"))
                .isTrue();
        assertThat(SensitiveConfigUtils.isSensitive("fs.s3a.encryption.key")).isTrue();
    }

    @Test
    void testIsSensitiveIgnoresNonCredentialKeys() {
        assertThat(SensitiveConfigUtils.isSensitive("bucket")).isFalse();
        assertThat(SensitiveConfigUtils.isSensitive("warehouse")).isFalse();
        assertThat(SensitiveConfigUtils.isSensitive("fs.oss.endpoint")).isFalse();
        assertThat(SensitiveConfigUtils.isSensitive("metastore")).isFalse();
        assertThat(SensitiveConfigUtils.isSensitive("")).isFalse();
        assertThat(SensitiveConfigUtils.isSensitive(null)).isFalse();
    }

    @Test
    void testRedactValue() {
        // Secret/access-key values keep only their last 4 chars.
        assertThat(SensitiveConfigUtils.redactValue("fs.oss.accessKeySecret", "0123456789abcdef"))
                .isEqualTo("****cdef");
        // Short value is fully masked.
        assertThat(SensitiveConfigUtils.redactValue("password", "short")).isEqualTo(REDACTED);
        // Non-sensitive key is untouched.
        assertThat(SensitiveConfigUtils.redactValue("bucket", "my-bucket")).isEqualTo("my-bucket");
    }

    @Test
    void testPasswordAndTokenAreFullyMasked() {
        // Passwords and tokens leak too much from a trailing hint -> always fully masked.
        assertThat(SensitiveConfigUtils.redactValue("my.password", "0123456789abcdef"))
                .isEqualTo(REDACTED);
        assertThat(SensitiveConfigUtils.redactValue("security.token", "0123456789abcdef"))
                .isEqualTo(REDACTED);
        assertThat(SensitiveConfigUtils.redactValue("Authorization", "Bearer 0123456789abcdef"))
                .isEqualTo(REDACTED);
        // Contrast: an access-key secret keeps its tail.
        assertThat(SensitiveConfigUtils.redactValue("fs.s3a.secret.key", "0123456789abcdef"))
                .isEqualTo("****cdef");
    }

    @Test
    void testRedactMapReplacesOnlySensitiveValues() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("warehouse", "mock://warehouse");
        options.put("fs.oss.accessKeyId", "mock-access-id-0001");
        options.put("fs.oss.accessKeySecret", "mock-secret-value");
        options.put("fs.azure.account.key.acct", "mock-azure-key-value");
        options.put("fs.oss.endpoint", "mock-endpoint.example.com");

        Map<String, String> redacted = SensitiveConfigUtils.redactMap(options);

        assertThat(redacted).containsEntry("warehouse", "mock://warehouse");
        // Long secrets keep their last 4 chars; endpoint is untouched.
        assertThat(redacted).containsEntry("fs.oss.accessKeyId", "****0001");
        assertThat(redacted).containsEntry("fs.oss.accessKeySecret", "****alue");
        assertThat(redacted).containsEntry("fs.azure.account.key.acct", "****alue");
        assertThat(redacted).containsEntry("fs.oss.endpoint", "mock-endpoint.example.com");
        // Original map must not be mutated.
        assertThat(options).containsEntry("fs.oss.accessKeySecret", "mock-secret-value");
        // The rendered string must not contain any raw secret.
        assertThat(redacted.toString())
                .doesNotContain("mock-secret-value")
                .doesNotContain("mock-access-id-0001");
    }

    @Test
    void testRedactMapNullSafe() {
        assertThat(SensitiveConfigUtils.redactMap(null)).isNull();
    }

    @Test
    void testRedactTextMasksSecretsInJsonAndForm() {
        String json =
                "{\"accessKeyId\":\"mock-id\",\"accessKeySecret\":\"mock-secret-abcd\","
                        + "\"securityToken\":\"mock-tok\",\"endpoint\":\"mock-endpoint\"}";
        String redactedJson = SensitiveConfigUtils.redactText(json);
        assertThat(redactedJson)
                .doesNotContain("mock-secret-abcd")
                .doesNotContain("mock-tok")
                .contains(REDACTED)
                // Non-sensitive fields are kept.
                .contains("mock-endpoint");

        String form = "password=mock-pass&user=alice&fs.oss.access-key=mock-ak";
        String redactedForm = SensitiveConfigUtils.redactText(form);
        assertThat(redactedForm)
                .doesNotContain("mock-pass")
                .doesNotContain("mock-ak")
                .contains("user=alice");
    }

    @Test
    void testRedactTextMasksWholeValueWithSpaces() {
        // The token after the "Bearer " scheme (with a space) must be masked, not just "Bearer".
        String header = "Authorization: Bearer mock-jwt-token-abcdef";
        assertThat(SensitiveConfigUtils.redactText(header))
                .doesNotContain("mock-jwt-token-abcdef")
                .startsWith("Authorization: ");

        // A quoted multi-word secret value is masked up to the closing quote only.
        String json = "{\"token\":\"Bearer mock-jwt-abcd\",\"user\":\"alice\"}";
        assertThat(SensitiveConfigUtils.redactText(json))
                .doesNotContain("mock-jwt-abcd")
                .contains("\"user\":\"alice\"");
    }

    @Test
    void testRedactTextNullAndEmptySafe() {
        assertThat(SensitiveConfigUtils.redactText(null)).isNull();
        assertThat(SensitiveConfigUtils.redactText("")).isEmpty();
        assertThat(SensitiveConfigUtils.redactText("no secrets here")).isEqualTo("no secrets here");
    }
}
