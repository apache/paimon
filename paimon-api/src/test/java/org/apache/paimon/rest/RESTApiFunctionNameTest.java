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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.rest.RESTCatalogInternalOptions.PREFIX;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the name check {@link RESTApi} makes before it sends anything. */
public class RESTApiFunctionNameTest {

    @Test
    public void testARejectedFunctionNameIsReportedAndNotFormatted() {
        Options options = new Options();
        options.set(URI, "http://127.0.0.1:1");
        options.set(TOKEN_PROVIDER, "bear");
        options.set(TOKEN, "secret");
        options.set(PREFIX, "catalog");
        RESTApi api = new RESTApi(options, false);

        // the name is why the request is refused, so it belongs in the message as data -- the
        // validator rejects '%', which is also what java.util.Formatter reads as a conversion
        assertThatThrownBy(() -> api.getFunction(Identifier.create("db", "function%")))
                .isInstanceOf(NoSuchResourceException.class)
                .hasMessageContaining("function%");
    }
}
