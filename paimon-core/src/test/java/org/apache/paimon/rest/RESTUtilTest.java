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

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link RESTUtil}. */
public class RESTUtilTest {
    @Test
    public void testMerge() {
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("key1", "default1");
            targets.put("key2", "default2");
            Map<String, String> updates = new HashMap<>();
            updates.put("key2", "update2");
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("key1"), "default1");
            assertEquals(result.get("key2"), "update2");
        }
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("key1", "default1");
            targets.put("key2", "default2");
            Map<String, String> updates = new HashMap<>();
            updates.put("key1", "default1");
            updates.put("key2", "update2");
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("key1"), "default1");
            assertEquals(result.get("key2"), "update2");
        }
        {
            Map<String, String> targets = new HashMap<>();
            targets.put("key1", "default1");
            targets.put("key2", "default2");
            Map<String, String> updates = new HashMap<>();
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("key1"), "default1");
            assertEquals(result.get("key2"), "default2");
        }
        {
            Map<String, String> targets = new HashMap<>();
            Map<String, String> updates = new HashMap<>();
            updates.put("key2", "update2");
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.get("key2"), "update2");
        }
        {
            Map<String, String> targets = new HashMap<>();
            Map<String, String> updates = new HashMap<>();
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.size(), 0);
        }
        {
            Map<String, String> targets = null;
            Map<String, String> updates = null;
            Map<String, String> result = RESTUtil.merge(targets, updates);
            assertEquals(result.size(), 0);
        }
    }
}
