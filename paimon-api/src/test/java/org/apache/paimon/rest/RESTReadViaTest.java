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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the REST read-via identifier protocol. */
public class RESTReadViaTest {

    @Test
    public void testReadViaDisabledByDefault() {
        assertThat(new Options().get(RESTCatalogOptions.READ_VIA_ENABLED)).isFalse();
    }

    @Test
    public void testIdentifierRoundTrip() {
        Identifier table = Identifier.create("db_table", "my_table");
        Identifier view = Identifier.create("db_view", "my_view");

        Identifier marked = RESTReadVia.withReadVia(table, view);
        RESTReadVia parsed = RESTReadVia.parse(marked);

        assertThat(marked.getFullName()).isEqualTo("db_table.my_table$via_db_view.my_view");
        assertThat(parsed.identifier()).isEqualTo(table);
        assertThat(parsed.readVia()).contains(view);
    }

    @Test
    public void testOrdinaryIdentifierHasNoReadVia() {
        Identifier table = Identifier.create("db", "my_table");

        RESTReadVia parsed = RESTReadVia.parse(table);

        assertThat(parsed.identifier()).isEqualTo(table);
        assertThat(parsed.readVia()).isEmpty();
    }

    @Test
    public void testBranchAndSystemTableRoundTrip() {
        Identifier table = Identifier.create("db", "my_table$branch_dev$files");
        Identifier view = Identifier.create("db", "my_view");

        Identifier marked = RESTReadVia.withReadVia(table, view);
        RESTReadVia parsed = RESTReadVia.parse(marked);

        assertThat(marked.getFullName()).isEqualTo("db.my_table$branch_dev$files$via_db.my_view");
        assertThat(parsed.identifier()).isEqualTo(table);
        assertThat(parsed.readVia()).contains(view);
    }

    @Test
    public void testWithReadViaKeepsOutermostRoot() {
        Identifier table = Identifier.create("db", "my_table");
        Identifier outerView = Identifier.create("db", "outer_view");
        Identifier innerView = Identifier.create("db", "inner_view");

        Identifier marked = RESTReadVia.withReadVia(table, outerView);

        assertThat(RESTReadVia.withReadVia(marked, innerView)).isEqualTo(marked);
    }

    @Test
    public void testRejectMultipleReadViaMarkers() {
        Identifier identifier =
                Identifier.create("db", "my_table$via_db.outer_view$via_db.inner_view");

        assertThatThrownBy(() -> RESTReadVia.parse(identifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid REST read-via identifier");
    }

    @Test
    public void testRejectEmptyReadViaRoot() {
        Identifier identifier = Identifier.create("db", "my_table$via_");

        assertThatThrownBy(() -> RESTReadVia.parse(identifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid REST read-via identifier");
    }

    @Test
    public void testRejectIncompleteReadViaRoot() {
        Identifier identifier = Identifier.create("db", "my_table$via_db.");

        assertThatThrownBy(() -> RESTReadVia.parse(identifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid REST read-via identifier");
    }
}
