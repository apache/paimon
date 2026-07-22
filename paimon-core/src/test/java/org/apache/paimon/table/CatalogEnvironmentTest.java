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

package org.apache.paimon.table;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.ReadAuthorizationContext;
import org.apache.paimon.catalog.ReadAuthorizationRootType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CatalogEnvironment}. */
class CatalogEnvironmentTest {

    private static final Identifier TABLE = Identifier.create("db", "table");
    private static final Identifier VIEW = Identifier.create("db", "view");

    @Test
    void testDirectTableCreatesFreshDependencyContextPerRead() {
        CatalogEnvironment environment =
                new CatalogEnvironment(TABLE, null, null, null, null, null, false, false);

        ReadAuthorizationContext first = environment.dependencyReadContext();
        ReadAuthorizationContext second = environment.dependencyReadContext();

        assertThat(first).isNotSameAs(second);
        assertThat(first.authorizationRootType()).contains(ReadAuthorizationRootType.TABLE);
        assertThat(first.authorizationRoot()).contains(TABLE);
        assertThat(first.readGrant()).isEmpty();
    }

    @Test
    void testDependencyReachedThroughViewPreservesOutermostRoot() {
        ReadAuthorizationContext viewContext = ReadAuthorizationContext.forView(VIEW);
        CatalogEnvironment environment =
                new CatalogEnvironment(
                        TABLE, null, null, null, null, null, false, false, viewContext);

        assertThat(environment.dependencyReadContext()).isSameAs(viewContext);
    }

    @Test
    void testEnvironmentWithoutIdentifierRemainsDirect() {
        assertThat(CatalogEnvironment.empty().dependencyReadContext().isDirect()).isTrue();
    }
}
