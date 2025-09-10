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

import org.apache.paimon.rest.RESTUtil;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.rest.RESTCatalogOptions.DLF_OSS_ENDPOINT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link RESTUtil}. */
public class RESTUtilTest {
    @Test
    public void testMerge() {
        // Test case 1: targets has precedence over updates for existing keys
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("key1", "default1");
            targets.put("key2", "default2");
            Map<String, String> updates = new HashMap<>();
            updates.put("key2", "update2");
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("key1"), "default1");
            // key2 should keep targets value, not be overridden by updates
            assertEquals(result.get("key2"), "default2");
        }

        // Test case 2: targets has precedence even when updates has same value
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("key1", "default1");
            targets.put("key2", "default2");
            Map<String, String> updates = new HashMap<>();
            updates.put("key1", "default1");
            updates.put("key2", "update2");
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("key1"), "default1");
            // key2 should keep targets value
            assertEquals(result.get("key2"), "default2");
        }

        // Test case 3: empty updates, targets unchanged
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("key1", "default1");
            targets.put("key2", "default2");
            Map<String, String> updates = new HashMap<>();
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("key1"), "default1");
            assertEquals(result.get("key2"), "default2");
        }

        // Test case 4: empty targets, updates are added
        {
            Map<String, String> targets = new HashMap<>();
            Map<String, String> updates = new HashMap<>();
            updates.put("key2", "update2");
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("key2"), "update2");
        }

        // Test case 5: both empty
        {
            Map<String, String> targets = new HashMap<>();
            Map<String, String> updates = new HashMap<>();
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.size(), 0);
        }

        // Test case 6: both null
        {
            Map<String, String> targets = null;
            Map<String, String> updates = null;
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.size(), 0);
        }

        // Test case 7: null values are ignored
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("key3", null);
            Map<String, String> updates = new HashMap<>();
            updates.put("key2", null);
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.size(), 0);
        }

        // Test case 8: updates adds new keys that don't exist in targets
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("key1", "default1");
            Map<String, String> updates = new HashMap<>();
            updates.put("key2", "update2");
            updates.put("key3", "update3");
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("key1"), "default1");
            assertEquals(result.get("key2"), "update2");
            assertEquals(result.get("key3"), "update3");
            assertEquals(result.size(), 3);
        }
    }

    @Test
    public void testMergeDlfOssEndpointSpecialCase() {
        // Test case 1: dlf.oss-endpoint overrides fs.oss.endpoint when present and non-empty
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("fs.oss.endpoint", "original-endpoint");
            targets.put("other.config", "value1");
            Map<String, String> updates = new HashMap<>();
            updates.put(DLF_OSS_ENDPOINT.key(), "new-oss-endpoint");
            updates.put("other.config", "value2"); // This should not override
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("fs.oss.endpoint"), "new-oss-endpoint");
            assertEquals(result.get("other.config"), "value1"); // targets takes precedence
            assertEquals(result.size(), 3);
        }

        // Test case 2: dlf.oss-endpoint adds fs.oss.endpoint when not present in targets
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("other.config", "value1");
            Map<String, String> updates = new HashMap<>();
            updates.put(DLF_OSS_ENDPOINT.key(), "new-oss-endpoint");
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("fs.oss.endpoint"), "new-oss-endpoint");
            assertEquals(result.get("other.config"), "value1");
            assertEquals(result.size(), 3);
        }

        // Test case 3: Empty dlf.oss-endpoint should not override
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("fs.oss.endpoint", "original-endpoint");
            Map<String, String> updates = new HashMap<>();
            updates.put(DLF_OSS_ENDPOINT.key(), "");
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("fs.oss.endpoint"), "original-endpoint");
        }

        // Test case 4: Null dlf.oss-endpoint should not override
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("fs.oss.endpoint", "original-endpoint");
            Map<String, String> updates = new HashMap<>();
            updates.put(DLF_OSS_ENDPOINT.key(), null);
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("fs.oss.endpoint"), "original-endpoint");
        }

        // Test case 5: No dlf.oss-endpoint in updates, fs.oss.endpoint unchanged
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("fs.oss.endpoint", "original-endpoint");
            Map<String, String> updates = new HashMap<>();
            updates.put("other.config", "value1");
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("fs.oss.endpoint"), "original-endpoint");
            assertEquals(result.get("other.config"), "value1");
        }
    }
}
