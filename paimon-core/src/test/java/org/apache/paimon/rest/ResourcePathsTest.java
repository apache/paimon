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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test for {@link ResourcePaths}. */
public class ResourcePathsTest {
    private final String prefix = "test";
    private ResourcePaths resourcePaths = ResourcePaths.forCatalogProperties(prefix);

    @Test
    public void configPathTest() {
        ResourcePaths resourcePaths = ResourcePaths.forCatalogProperties(prefix);
        String expected = replacePrefix(ResourcePaths.V1_CONFIG, prefix);
        assertEquals(expected, "/" + resourcePaths.config());
    }

    private String replacePrefix(String path, String prefix) {
        return path.replace("{prefix}", prefix);
    }
}
