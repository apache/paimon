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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.RESTTestFileIO;
import org.apache.paimon.rest.auth.AuthProviderEnum;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** Test for {@link FlinkCatalog} with Rest metastore. */
public class FlinkRestCatalogITCase extends RESTCatalogITCaseBase {

    private FlinkCatalog catalog;

    @BeforeEach
    @Override
    public void before() throws IOException {
        super.before();
        initFlinkCatalog();
    }

    @Test
    void testListTableReturnsView() throws Exception {
        catalog.createDatabase("test", null, true);
        catalog.createTable(
                new ObjectPath("test", "t1"), createTable(Collections.emptyMap()), true);
        assertThat(catalog.listTables("test")).containsExactly("t1");
        catalog.createTable(new ObjectPath("test", "v"), createView(), true);
        assertThat(catalog.listTables("test")).containsExactlyInAnyOrder("t1", "v");
    }

    private CatalogTable createTable(Map<String, String> options) {
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("test comment")
                        .partitionKeys(Collections.emptyList())
                        .options(options)
                        .build();
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    private CatalogView createView() {
        ResolvedSchema resolvedSchema = this.createSchema();

        String query = "SELECT * FROM test.t1";
        CatalogView view =
                CatalogView.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        null,
                        query,
                        query,
                        Collections.emptyMap());
        return new ResolvedCatalogView(view, resolvedSchema);
    }

    private ResolvedSchema createSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("first", org.apache.flink.table.api.DataTypes.STRING()),
                        Column.physical("second", org.apache.flink.table.api.DataTypes.INT()),
                        Column.physical("third", org.apache.flink.table.api.DataTypes.STRING()),
                        Column.physical(
                                "four",
                                org.apache.flink.table.api.DataTypes.ROW(
                                        org.apache.flink.table.api.DataTypes.FIELD(
                                                "f1",
                                                org.apache.flink.table.api.DataTypes.STRING()),
                                        org.apache.flink.table.api.DataTypes.FIELD(
                                                "f2", org.apache.flink.table.api.DataTypes.INT()),
                                        org.apache.flink.table.api.DataTypes.FIELD(
                                                "f3",
                                                org.apache.flink.table.api.DataTypes.MAP(
                                                        org.apache.flink.table.api.DataTypes
                                                                .STRING(),
                                                        DataTypes.INT()))))),
                Collections.emptyList(),
                null);
    }

    private void initFlinkCatalog() {
        Map<String, String> authMap =
                ImmutableMap.of(
                        RESTCatalogOptions.TOKEN.key(),
                        RESTCatalogITCaseBase.INIT_TOKEN,
                        RESTCatalogOptions.TOKEN_PROVIDER.key(),
                        AuthProviderEnum.BEAR.identifier());
        Options options = new Options();
        for (Map.Entry<String, String> entry : authMap.entrySet()) {
            options.set(entry.getKey(), entry.getValue());
        }
        options.set(CatalogOptions.METASTORE, "rest");
        options.set(CatalogOptions.WAREHOUSE.key(), warehouse);
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTTestFileIO.DATA_PATH_CONF_KEY, path);
        catalog =
                FlinkCatalogFactory.createCatalog(
                        "c",
                        CatalogContext.create(options),
                        FlinkRestCatalogITCase.class.getClassLoader());
    }
}
