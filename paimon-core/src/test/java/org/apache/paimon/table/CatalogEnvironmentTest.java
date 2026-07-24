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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTCatalogFactory;
import org.apache.paimon.rest.RESTCatalogLoader;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link CatalogEnvironment}. */
class CatalogEnvironmentTest {

    private static final String READ_VIA_OPTION = RESTApi.HEADER_PREFIX + RESTApi.READ_VIA_HEADER;

    @Test
    void testDependencyReadContextForRestCatalog() {
        Identifier root = Identifier.create("db", "root$branch_dev");
        Options options = new Options();
        options.set("other-option", "value");
        CatalogContext context = CatalogContext.create(options);
        CatalogEnvironment environment = restEnvironment(root, context);

        CatalogContext dependencyContext = environment.dependencyReadContext();

        assertThat(dependencyContext).isNotSameAs(context);
        assertThat(context.options().containsKey(READ_VIA_OPTION)).isFalse();
        assertThat(dependencyContext.options().get(METASTORE))
                .isEqualTo(RESTCatalogFactory.IDENTIFIER);
        assertThat(dependencyContext.options().get("other-option")).isEqualTo("value");
        Identifier readVia =
                JsonSerdeUtil.fromJson(
                        RESTUtil.decodeString(dependencyContext.options().get(READ_VIA_OPTION)),
                        Identifier.class);
        assertThat(readVia).isEqualTo(root);
    }

    @Test
    void testDependencyReadContextPreservesOutermostTable() {
        Identifier outermost = Identifier.create("db", "outermost");
        Options options = new Options();
        options.set(READ_VIA_OPTION, RESTUtil.encodeString(JsonSerdeUtil.toFlatJson(outermost)));
        CatalogContext context = CatalogContext.create(options);
        CatalogEnvironment environment =
                restEnvironment(Identifier.create("db", "intermediate"), context);

        assertThat(environment.dependencyReadContext()).isSameAs(context);
        assertThat(context.options().get(READ_VIA_OPTION))
                .isEqualTo(RESTUtil.encodeString(JsonSerdeUtil.toFlatJson(outermost)));
    }

    @Test
    void testDependencyReadContextDoesNotAffectOtherCatalogs() {
        CatalogContext context = CatalogContext.create(new Options());
        CatalogEnvironment environment = environment(Identifier.create("db", "table"), context);

        assertThat(environment.dependencyReadContext()).isSameAs(context);
        assertThat(context.options().containsKey(READ_VIA_OPTION)).isFalse();
    }

    @Test
    void testDependencyReadContextForExternalRestTable() {
        Options options = new Options();
        options.set(METASTORE, RESTCatalogFactory.IDENTIFIER);
        CatalogContext context = CatalogContext.create(options);
        CatalogEnvironment environment = environment(Identifier.create("db", "external"), context);

        assertThat(environment.dependencyReadContext()).isNotSameAs(context);
    }

    @Test
    void testAppendTableUsesDependencyReadContext() {
        CatalogEnvironment environment = mock(CatalogEnvironment.class);
        when(environment.dependencyReadContext()).thenReturn(CatalogContext.create(new Options()));
        TableSchema schema =
                new TableSchema(
                        0,
                        Collections.singletonList(new DataField(0, "id", DataTypes.INT())),
                        0,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true"),
                        null);
        AppendOnlyFileStoreTable table =
                new AppendOnlyFileStoreTable(
                        mock(FileIO.class), new Path("file:/tmp/table"), schema, environment);

        table.newRead();

        verify(environment).dependencyReadContext();
    }

    private static CatalogEnvironment environment(
            Identifier identifier, CatalogContext catalogContext) {
        return new CatalogEnvironment(
                identifier, null, null, null, null, catalogContext, false, false);
    }

    private static CatalogEnvironment restEnvironment(
            Identifier identifier, CatalogContext catalogContext) {
        return new CatalogEnvironment(
                identifier,
                null,
                new RESTCatalogLoader(catalogContext),
                null,
                null,
                catalogContext,
                false,
                false);
    }
}
