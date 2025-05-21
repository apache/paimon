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

package org.apache.paimon.iceberg;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

public class IcebergRESTPlayground {

    public static void main(String[] args) {
        // Create and initialize the REST catalog
        RESTCatalog catalog = createRestCatalog();
        createExampleTable(catalog);
    }

    private static void createExampleTable(RESTCatalog catalog) {
        // Define the namespace and table name
        Namespace namespace = Namespace.of("ndelnano");
        TableIdentifier tableId = TableIdentifier.of(namespace, "iceberg_rest_java");

        // Check if the namespace exists, create it if it doesn't
        if (!catalog.namespaceExists(namespace)) {
            System.out.println("Creating namespace: " + namespace);
            catalog.createNamespace(namespace);
        }

        // Define the schema for the table
        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.LongType.get()),
                        Types.NestedField.required(2, "name", Types.StringType.get()),
                        Types.NestedField.optional(3, "data", Types.StringType.get()),
                        Types.NestedField.required(4, "timestamp", Types.TimestampType.withZone()));

        // Define the partition spec (or use PartitionSpec.unpartitioned() for no partitioning)
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("timestamp").build();

        // Define table properties
        Map<String, String> properties = new HashMap<>();
        properties.put("write.format.default", "parquet");
        properties.put("write.parquet.compression-codec", "snappy");

        // Define the storage location (optional, catalog can choose default location)
        String location = "s3://my-bucket/my-warehouse/my_database.db/my_new_table";

        // Create the table
        System.out.println("Creating table: " + tableId);

        Table table =
                catalog.buildTable(tableId, schema)
                        .withPartitionSpec(spec)
                        .withLocation(location)
                        .withProperties(properties)
                        .create();

        System.out.println("Successfully created table: " + table.name());
    }

    private static RESTCatalog createRestCatalog() {
        // Create a new REST catalog
        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.URI, "https://glue.us-west-2.amazonaws.com/iceberg");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "528741615426");

        // AWS Region for signing requests
        properties.put(AwsProperties.REST_SIGNER_REGION, "us-west-2"); // Replace with your region
        properties.put(AwsProperties.REST_SIGNING_NAME, "glue");
        properties.put("client.region", "us-west-2");
        // region

        // Optional: Configure catalog caching
        properties.put(CatalogProperties.CACHE_ENABLED, "true");
        properties.put(CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS, "300000"); // 5 minutes

        // Optional: Configure client pool
        properties.put(CatalogProperties.CLIENT_POOL_SIZE, "5");

        // Initialize the catalog with a name and properties
        catalog.initialize("glue-rest-catalog", properties);

        return catalog;
    }
}
