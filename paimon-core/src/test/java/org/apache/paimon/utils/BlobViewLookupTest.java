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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlobViewLookup}. */
class BlobViewLookupTest {

    @Test
    void testOriginViewTableOptionDoesNotMutateOriginalContext() {
        Options options = new Options();
        options.set(WAREHOUSE, "file:/tmp/warehouse");
        CatalogContext context = CatalogContext.create(options);

        CatalogContext lookupContext =
                BlobViewLookup.withOriginViewTable(
                        context, Identifier.create("default", "downstream"));

        assertThat(lookupContext).isNotSameAs(context);
        assertThat(lookupContext.options().get(BlobViewLookup.ORIGIN_VIEW_TABLE))
                .isEqualTo("default.downstream");
        assertThat(lookupContext.options().get(WAREHOUSE)).isEqualTo("file:/tmp/warehouse");
        assertThat(context.options().get(BlobViewLookup.ORIGIN_VIEW_TABLE)).isNull();
    }

    @Test
    void testOriginViewTableOptionSkippedWhenOriginTableIsUnknown() {
        CatalogContext context = CatalogContext.create(new Options());

        assertThat(BlobViewLookup.withOriginViewTable(context, null)).isSameAs(context);
    }
}
