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

package org.apache.paimon.rest.auth;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Test for {@link RESTAuthParameter}. */
public class RESTAuthParameterTest {

    @Test
    public void testParameterHasNoApiNameByDefault() {
        RESTAuthParameter parameter =
                new RESTAuthParameter("/v1/config", Collections.emptyMap(), "GET", null);

        assertNull(parameter.apiName());
    }

    /** The copy keeps the encoded parameters as they are, rather than encoding them again. */
    @Test
    public void testWithApiNameDoesNotEncodeParametersTwice() {
        RESTAuthParameter parameter =
                new RESTAuthParameter(
                        "/v1/prefix/databases",
                        Collections.singletonMap("databaseNamePattern", "db %"),
                        "GET",
                        "body");

        RESTAuthParameter named = parameter.withApiName("ListDatabases");

        assertEquals("ListDatabases", named.apiName());
        assertEquals(parameter.parameters(), named.parameters());
        assertEquals(parameter.resourcePath(), named.resourcePath());
        assertEquals(parameter.method(), named.method());
        assertEquals(parameter.data(), named.data());
        assertNull(parameter.apiName());
    }
}
