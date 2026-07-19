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
        // Long value keeps only its last 4 chars.
        assertThat(SensitiveConfigUtils.redactValue("fs.oss.accessKeySecret", "0123456789abcdef"))
                .isEqualTo("****cdef");
        // Short value is fully masked.
        assertThat(SensitiveConfigUtils.redactValue("password", "short")).isEqualTo(REDACTED);
        // Non-sensitive key is untouched.
        assertThat(SensitiveConfigUtils.redactValue("bucket", "my-bucket")).isEqualTo("my-bucket");
    }

    @Test
    void testRedactMapReplacesOnlySensitiveValues() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("warehouse", "oss://bucket/warehouse");
        options.put("fs.oss.accessKeyId", "LTAI-secret-id-01");
        options.put("fs.oss.accessKeySecret", "real-secret-value");
        options.put("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");

        Map<String, String> redacted = SensitiveConfigUtils.redactMap(options);

        assertThat(redacted).containsEntry("warehouse", "oss://bucket/warehouse");
        // Long secrets keep their last 4 chars; endpoint is untouched.
        assertThat(redacted).containsEntry("fs.oss.accessKeyId", "****d-01");
        assertThat(redacted).containsEntry("fs.oss.accessKeySecret", "****alue");
        assertThat(redacted).containsEntry("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        // Original map must not be mutated.
        assertThat(options).containsEntry("fs.oss.accessKeySecret", "real-secret-value");
        // The rendered string must not contain any raw secret.
        assertThat(redacted.toString())
                .doesNotContain("real-secret-value")
                .doesNotContain("LTAI-secret-id-01");
    }

    @Test
    void testRedactMapNullSafe() {
        assertThat(SensitiveConfigUtils.redactMap(null)).isNull();
    }

    @Test
    void testRedactTextMasksSecretsInJsonAndForm() {
        String json =
                "{\"accessKeyId\":\"LTAI-id\",\"accessKeySecret\":\"the-real-secret\","
                        + "\"securityToken\":\"tok-123\",\"endpoint\":\"oss-cn-hangzhou\"}";
        String redactedJson = SensitiveConfigUtils.redactText(json);
        assertThat(redactedJson)
                .doesNotContain("the-real-secret")
                .doesNotContain("tok-123")
                .contains(REDACTED)
                // Non-sensitive fields are kept.
                .contains("oss-cn-hangzhou");

        String form = "password=hunter2&user=alice&fs.oss.access-key=AK-9";
        String redactedForm = SensitiveConfigUtils.redactText(form);
        assertThat(redactedForm)
                .doesNotContain("hunter2")
                .doesNotContain("AK-9")
                .contains("user=alice");
    }

    @Test
    void testRedactTextNullAndEmptySafe() {
        assertThat(SensitiveConfigUtils.redactText(null)).isNull();
        assertThat(SensitiveConfigUtils.redactText("")).isEmpty();
        assertThat(SensitiveConfigUtils.redactText("no secrets here")).isEqualTo("no secrets here");
    }
}
