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

package org.apache.paimon.rest;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.rest.RESTCatalogOptions.DLF_OSS_ENDPOINT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link RESTTokenFileIO}. */
public class RESTTokenFileIOTest {
    @Test
    public void testMergeTokenWithDlfEndpointHandling() {
        // Test case 1: dlf.oss-endpoint overrides fs.oss.endpoint when present and non-empty
        {
            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put("fs.oss.endpoint", "original-endpoint");
            catalogProperties.put("other.config", "value1");
            Map<String, String> restTokenProperties = new HashMap<>();
            restTokenProperties.put(DLF_OSS_ENDPOINT.key(), "new-oss-endpoint");
            restTokenProperties.put("other.config", "value2"); // This should override
            Map<String, String> result =
                    RESTTokenFileIO.mergeTokenWithDlfEndpointHandling(
                            catalogProperties, restTokenProperties);
            assertEquals("new-oss-endpoint", result.get("fs.oss.endpoint"));
            assertEquals(
                    "value2", result.get("other.config")); // restTokenProperties takes precedence
            assertEquals("new-oss-endpoint", result.get(DLF_OSS_ENDPOINT.key()));
            assertEquals(3, result.size());
        }

        // Test case 2: dlf.oss-endpoint adds fs.oss.endpoint when not present in catalogProperties
        {
            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put("other.config", "value1");
            Map<String, String> restTokenProperties = new HashMap<>();
            restTokenProperties.put(DLF_OSS_ENDPOINT.key(), "new-oss-endpoint");
            Map<String, String> result =
                    RESTTokenFileIO.mergeTokenWithDlfEndpointHandling(
                            catalogProperties, restTokenProperties);
            assertEquals("new-oss-endpoint", result.get("fs.oss.endpoint"));
            assertEquals("value1", result.get("other.config"));
            assertEquals("new-oss-endpoint", result.get(DLF_OSS_ENDPOINT.key()));
            assertEquals(3, result.size());
        }

        // Test case 3: Empty dlf.oss-endpoint should not override
        {
            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put("fs.oss.endpoint", "original-endpoint");
            Map<String, String> restTokenProperties = new HashMap<>();
            restTokenProperties.put(DLF_OSS_ENDPOINT.key(), "");
            Map<String, String> result =
                    RESTTokenFileIO.mergeTokenWithDlfEndpointHandling(
                            catalogProperties, restTokenProperties);
            assertEquals("original-endpoint", result.get("fs.oss.endpoint"));
            assertEquals(
                    2, result.size()); // fs.oss.endpoint and dlf.oss-endpoint (empty string is not
            // null)
        }

        // Test case 4: Null dlf.oss-endpoint should not override
        {
            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put("fs.oss.endpoint", "original-endpoint");
            Map<String, String> restTokenProperties = new HashMap<>();
            restTokenProperties.put(DLF_OSS_ENDPOINT.key(), null);
            Map<String, String> result =
                    RESTTokenFileIO.mergeTokenWithDlfEndpointHandling(
                            catalogProperties, restTokenProperties);
            assertEquals("original-endpoint", result.get("fs.oss.endpoint"));
            assertEquals(1, result.size()); // Only fs.oss.endpoint (null values are filtered out)
        }

        // Test case 5: No dlf.oss-endpoint in restTokenProperties, fs.oss.endpoint unchanged
        {
            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put("fs.oss.endpoint", "original-endpoint");
            Map<String, String> restTokenProperties = new HashMap<>();
            restTokenProperties.put("other.config", "value1");
            Map<String, String> result =
                    RESTTokenFileIO.mergeTokenWithDlfEndpointHandling(
                            catalogProperties, restTokenProperties);
            assertEquals("original-endpoint", result.get("fs.oss.endpoint"));
            assertEquals("value1", result.get("other.config"));
            assertEquals(2, result.size());
        }

        // Test case 6: Both empty maps
        {
            Map<String, String> catalogProperties = new HashMap<>();
            Map<String, String> restTokenProperties = new HashMap<>();
            Map<String, String> result =
                    RESTTokenFileIO.mergeTokenWithDlfEndpointHandling(
                            catalogProperties, restTokenProperties);
            assertEquals(0, result.size());
        }

        // Test case 7: Both null maps
        {
            Map<String, String> result =
                    RESTTokenFileIO.mergeTokenWithDlfEndpointHandling(null, null);
            assertEquals(0, result.size());
        }

        // Test case 8: restTokenProperties adds new keys that don't exist in catalogProperties
        {
            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put("key1", "catalog1");
            Map<String, String> restTokenProperties = new HashMap<>();
            restTokenProperties.put("key2", "token2");
            restTokenProperties.put("key3", "token3");
            restTokenProperties.put(DLF_OSS_ENDPOINT.key(), "dlf-endpoint");
            Map<String, String> result =
                    RESTTokenFileIO.mergeTokenWithDlfEndpointHandling(
                            catalogProperties, restTokenProperties);
            assertEquals("catalog1", result.get("key1"));
            assertEquals("token2", result.get("key2"));
            assertEquals("token3", result.get("key3"));
            assertEquals("dlf-endpoint", result.get(DLF_OSS_ENDPOINT.key()));
            assertEquals("dlf-endpoint", result.get("fs.oss.endpoint")); // DLF endpoint handling
            assertEquals(5, result.size());
        }
    }
}
