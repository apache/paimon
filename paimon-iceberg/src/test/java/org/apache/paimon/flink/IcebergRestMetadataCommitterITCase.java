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

import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.iceberg.IcebergRestMetadataCommitter;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogServer;
import org.apache.iceberg.rest.RESTServerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link IcebergRestMetadataCommitter}. */
public class IcebergRestMetadataCommitterITCase extends AbstractTestBase {

    @RegisterExtension
    private static final RESTServerExtension REST_SERVER_EXTENSION =
            new RESTServerExtension(
                    Map.of(
                            RESTCatalogServer.REST_PORT,
                            RESTServerExtension.FREE_PORT,
                            CatalogProperties.CLIENT_POOL_SIZE,
                            "1",
                            CatalogProperties.CATALOG_IMPL,
                            HadoopCatalog.class.getName()));

    @ParameterizedTest
    @ValueSource(strings = {"avro", "parquet", "orc"})
    public void testCommitToRestCatalog(String fileFormat) throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().parallelism(2).build();

        RESTCatalog restCatalog = REST_SERVER_EXTENSION.client();
        String restUri = restCatalog.properties().get(CatalogProperties.URI);
        String restWarehouse = restCatalog.properties().get(CatalogProperties.WAREHOUSE_LOCATION);
        String restClients = restCatalog.properties().get(CatalogProperties.CLIENT_POOL_SIZE);

        tEnv.executeSql(
                "CREATE CATALOG paimon WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE paimon.`default`.T (\n"
                                + "  pt INT,\n"
                                + "  k INT,\n"
                                + "  v INT,\n"
                                + "  PRIMARY KEY (pt, k) NOT ENFORCED\n"
                                + ") PARTITIONED BY (pt) WITH (\n"
                                + "  'metadata.iceberg.storage' = 'rest-catalog',\n"
                                + "  'metadata.iceberg.rest.uri' = '%s',\n"
                                + "  'metadata.iceberg.rest.warehouse' = '%s',\n"
                                + "  'metadata.iceberg.rest.clients' = '%s',\n"
                                + "  'file.format' = '%s'\n"
                                + ")",
                        restUri, restWarehouse, restClients, fileFormat));
        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.T VALUES "
                                + "(1, 9, 90), "
                                + "(1, 10, 100), "
                                + "(1, 11, 110), "
                                + "(2, 20, 200)")
                .await();

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG iceberg WITH (\n"
                                + "  'type' = 'iceberg',\n"
                                + "  'catalog-type' = 'rest',\n"
                                + "  'uri' = '%s',\n"
                                + "  'clients' = '%s',\n"
                                + "  'cache-enabled' = 'false'\n"
                                + ")",
                        restUri, restClients));
        assertThat(
                        collect(
                                tEnv.executeSql(
                                        "SELECT v, k, pt FROM iceberg.`default`.T ORDER BY pt, k")))
                .containsExactly(
                        Row.of(90, 9, 1),
                        Row.of(100, 10, 1),
                        Row.of(110, 11, 1),
                        Row.of(200, 20, 2));
    }

    private List<Row> collect(TableResult result) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }
        return rows;
    }
}
