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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.options.Options;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogServer;
import org.apache.iceberg.rest.RESTServerExtension;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergMetadataTest {
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

    protected static RESTCatalog restCatalog;

    @BeforeEach
    void setUp() {
        restCatalog = REST_SERVER_EXTENSION.client();
    }

    @Test
    @DisplayName("Test reading metadata from basic Iceberg table creation")
    void testReadBasicIcebergTableMetadata() throws Exception {
        // Create a basic Iceberg table
        Table icebergTable = createBasicIcebergTable("basic_table");

        // Read metadata using Paimon's IcebergMetadata
        IcebergMetadata paimonIcebergMetadata = readIcebergMetadata(icebergTable);

        // Verify basic properties
        assertThat(paimonIcebergMetadata.formatVersion()).isEqualTo(2);
        assertThat(paimonIcebergMetadata.tableUuid()).isNotNull();
        assertThat(paimonIcebergMetadata.location()).isNotNull();
        assertThat(paimonIcebergMetadata.currentSchemaId()).isEqualTo(0);
        assertThat(paimonIcebergMetadata.defaultSpecId()).isEqualTo(0);
        assertThat(paimonIcebergMetadata.defaultSortOrderId()).isEqualTo(0);

        // Verify schema
        assertThat(paimonIcebergMetadata.schemas()).hasSize(1);
        IcebergSchema schema = paimonIcebergMetadata.schemas().get(0);
        assertThat(schema.fields()).hasSize(3);

        // Verify field details
        assertThat(schema.fields().get(0).name()).isEqualTo("id");
        assertThat(schema.fields().get(0).type()).isEqualTo("int");
        assertThat(schema.fields().get(0).required()).isTrue();

        assertThat(schema.fields().get(1).name()).isEqualTo("name");
        assertThat(schema.fields().get(1).type()).isEqualTo("string");
        assertThat(schema.fields().get(1).required()).isTrue();

        assertThat(schema.fields().get(2).name()).isEqualTo("age");
        assertThat(schema.fields().get(2).type()).isEqualTo("int");
        assertThat(schema.fields().get(2).required()).isFalse();

        // Verify partition specs (should be unpartitioned)
        assertThat(paimonIcebergMetadata.partitionSpecs()).hasSize(1);
        assertThat(paimonIcebergMetadata.partitionSpecs().get(0).fields()).isEmpty();

        // Verify snapshots (should be empty initially)
        assertThat(paimonIcebergMetadata.snapshots()).isEmpty();
        assertThat(paimonIcebergMetadata.currentSnapshotId()).isEqualTo(-1);
    }

    @Test
    @DisplayName("Test reading metadata from partitioned Iceberg table")
    void testReadPartitionedIcebergTableMetadata() throws Exception {
        // Create a partitioned Iceberg table
        Table icebergTable = createPartitionedIcebergTable("partitioned_table");

        // Read metadata using Paimon's IcebergMetadata
        IcebergMetadata paimonIcebergMetadata = readIcebergMetadata(icebergTable);

        // Verify basic properties
        assertThat(paimonIcebergMetadata.formatVersion()).isEqualTo(2);
        assertThat(paimonIcebergMetadata.tableUuid()).isNotNull();
        assertThat(paimonIcebergMetadata.location()).isNotNull();

        // Verify schema
        assertThat(paimonIcebergMetadata.schemas()).hasSize(1);
        IcebergSchema schema = paimonIcebergMetadata.schemas().get(0);
        assertThat(schema.fields()).hasSize(4);

        // Verify field details
        assertThat(schema.fields().get(0).name()).isEqualTo("id");
        assertThat(schema.fields().get(1).name()).isEqualTo("name");
        assertThat(schema.fields().get(2).name()).isEqualTo("department");
        assertThat(schema.fields().get(3).name()).isEqualTo("salary");

        // Verify partition specs
        assertThat(paimonIcebergMetadata.partitionSpecs()).hasSize(1);
        IcebergPartitionSpec partitionSpec = paimonIcebergMetadata.partitionSpecs().get(0);
        assertThat(partitionSpec.fields()).hasSize(1);
        assertThat(partitionSpec.fields().get(0).name()).isEqualTo("department");
        assertThat(partitionSpec.fields().get(0).transform()).isEqualTo("identity");

        // Verify snapshots (should be empty initially)
        assertThat(paimonIcebergMetadata.snapshots()).isEmpty();
        assertThat(paimonIcebergMetadata.currentSnapshotId()).isEqualTo(-1);
    }

    @Test
    @DisplayName("Test reading metadata from sorted Iceberg table")
    void testReadSortedIcebergTableMetadata() throws Exception {
        // Create a sorted Iceberg table
        Table icebergTable = createSortedIcebergTable("sorted_table");

        // Read metadata using Paimon's IcebergMetadata
        IcebergMetadata paimonIcebergMetadata = readIcebergMetadata(icebergTable);

        // Verify basic properties
        assertThat(paimonIcebergMetadata.formatVersion()).isEqualTo(2);
        assertThat(paimonIcebergMetadata.tableUuid()).isNotNull();
        assertThat(paimonIcebergMetadata.location()).isNotNull();

        // Verify schema
        assertThat(paimonIcebergMetadata.schemas()).hasSize(1);
        IcebergSchema schema = paimonIcebergMetadata.schemas().get(0);
        assertThat(schema.fields()).hasSize(3);

        // Verify field details
        assertThat(schema.fields().get(0).name()).isEqualTo("id");
        assertThat(schema.fields().get(1).name()).isEqualTo("name");
        assertThat(schema.fields().get(2).name()).isEqualTo("score");

        // Verify sort orders
        assertThat(paimonIcebergMetadata.sortOrders()).hasSize(1);
        assertThat(paimonIcebergMetadata.defaultSortOrderId()).isEqualTo(1);

        // Verify snapshots (should be empty initially)
        assertThat(paimonIcebergMetadata.snapshots()).isEmpty();
        assertThat(paimonIcebergMetadata.currentSnapshotId()).isEqualTo(-1);
    }

    @Test
    @DisplayName("Test reading metadata after Iceberg table operations")
    void testReadMetadataAfterIcebergOperations() throws Exception {
        // Create a basic Iceberg table
        Table icebergTable = createBasicIcebergTable("operations_table");

        // Perform first append operation
        icebergTable
                .newFastAppend()
                .appendFile(
                        DataFiles.builder(PartitionSpec.unpartitioned())
                                .withPath("/path/to/data-a.parquet")
                                .withFileSizeInBytes(100)
                                .withRecordCount(10)
                                .build())
                .commit();

        // Read metadata after first operation
        IcebergMetadata paimonIcebergMetadata1 = readIcebergMetadata("operations_table");

        // Verify snapshots after first operation
        assertThat(paimonIcebergMetadata1.snapshots()).hasSize(1);
        assertThat(paimonIcebergMetadata1.currentSnapshotId()).isNotNull();
        assertThat(paimonIcebergMetadata1.snapshots().get(0).parentSnapshotId()).isNull();

        // Perform second append operation
        icebergTable
                .newFastAppend()
                .appendFile(
                        DataFiles.builder(PartitionSpec.unpartitioned())
                                .withPath("/path/to/data-b.parquet")
                                .withFileSizeInBytes(200)
                                .withRecordCount(20)
                                .build())
                .commit();

        // Read metadata after second operation
        IcebergMetadata paimonIcebergMetadata2 = readIcebergMetadata("operations_table");

        // Verify snapshots after second operation
        assertThat(paimonIcebergMetadata2.snapshots()).hasSize(2);
        assertThat(paimonIcebergMetadata2.currentSnapshotId())
                .isEqualTo(paimonIcebergMetadata2.snapshots().get(1).snapshotId());
        assertThat(paimonIcebergMetadata2.snapshots().get(1).parentSnapshotId())
                .isEqualTo(paimonIcebergMetadata2.snapshots().get(0).snapshotId());

        // Verify snapshot sequence numbers
        assertThat(paimonIcebergMetadata2.snapshots().get(0).sequenceNumber()).isEqualTo(1L);
        assertThat(paimonIcebergMetadata2.snapshots().get(1).sequenceNumber()).isEqualTo(2L);
    }

    @Test
    @DisplayName("Test reading metadata with Iceberg table properties")
    void testReadMetadataWithIcebergTableProperties() throws Exception {
        // Create Iceberg table with custom properties
        TableIdentifier identifier = TableIdentifier.of("testdb", "properties_table");
        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "name", Types.StringType.get()));

        Map<String, String> properties = new HashMap<>();
        properties.put("write.format.default", "parquet");
        properties.put("write.parquet.compression-codec", "snappy");
        properties.put("write.target-file-size-bytes", "134217728");

        Table icebergTable =
                restCatalog.buildTable(identifier, schema).withProperties(properties).create();

        // Read metadata using Paimon's IcebergMetadata
        IcebergMetadata paimonIcebergMetadata = readIcebergMetadata(icebergTable);

        // Verify properties
        assertThat(paimonIcebergMetadata.properties()).isNotEmpty();
        assertThat(paimonIcebergMetadata.properties().get("write.format.default"))
                .isEqualTo("parquet");
        assertThat(paimonIcebergMetadata.properties().get("write.parquet.compression-codec"))
                .isEqualTo("snappy");
        assertThat(paimonIcebergMetadata.properties().get("write.target-file-size-bytes"))
                .isEqualTo("134217728");
    }

    @Test
    @DisplayName("Test reading metadata with complex Iceberg schema")
    void testReadMetadataWithComplexIcebergSchema() throws Exception {
        // Create Iceberg table with complex schema
        TableIdentifier identifier = TableIdentifier.of("testdb", "complex_schema_table");
        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "name", Types.StringType.get()),
                        Types.NestedField.optional(
                                3,
                                "address",
                                Types.StructType.of(
                                        Types.NestedField.required(
                                                4, "street", Types.StringType.get()),
                                        Types.NestedField.required(
                                                5, "city", Types.StringType.get()),
                                        Types.NestedField.optional(
                                                6, "zipcode", Types.StringType.get()))),
                        Types.NestedField.optional(
                                7, "tags", Types.ListType.ofOptional(8, Types.StringType.get())),
                        Types.NestedField.optional(
                                9,
                                "metadata",
                                Types.MapType.ofOptional(
                                        10, 11, Types.StringType.get(), Types.StringType.get())));

        Table icebergTable = restCatalog.buildTable(identifier, schema).create();

        // Read metadata using Paimon's IcebergMetadata
        IcebergMetadata paimonIcebergMetadata = readIcebergMetadata(icebergTable);

        // Verify schema
        assertThat(paimonIcebergMetadata.schemas()).hasSize(1);
        IcebergSchema paimonIcebergSchema = paimonIcebergMetadata.schemas().get(0);
        assertThat(paimonIcebergSchema.fields()).hasSize(5);

        // Verify basic fields
        assertThat(paimonIcebergSchema.fields().get(0).name()).isEqualTo("id");
        assertThat(paimonIcebergSchema.fields().get(0).type()).isEqualTo("int");
        assertThat(paimonIcebergSchema.fields().get(1).name()).isEqualTo("name");
        assertThat(paimonIcebergSchema.fields().get(1).type()).isEqualTo("string");

        // Verify complex fields exist
        assertThat(paimonIcebergSchema.fields().get(2).name()).isEqualTo("address");
        assertThat(paimonIcebergSchema.fields().get(3).name()).isEqualTo("tags");
        assertThat(paimonIcebergSchema.fields().get(4).name()).isEqualTo("metadata");
    }

    @Test
    @DisplayName("Test reading metadata with Iceberg table evolution")
    void testReadMetadataWithIcebergTableEvolution() throws Exception {
        // Create initial Iceberg table
        Table icebergTable = createBasicIcebergTable("evolution_table");

        // Read initial metadata
        IcebergMetadata initialMetadata = readIcebergMetadata("evolution_table");
        assertThat(initialMetadata.schemas()).hasSize(1);
        assertThat(initialMetadata.schemas().get(0).fields()).hasSize(3);
        assertThat(initialMetadata.currentSchemaId()).isEqualTo(0);

        // Evolve schema by adding a new column
        icebergTable.updateSchema().addColumn("email", Types.StringType.get()).commit();

        // Read metadata after schema evolution
        IcebergMetadata evolvedMetadata = readIcebergMetadata("evolution_table");

        // Verify schema evolution
        assertThat(evolvedMetadata.schemas()).hasSize(2); // Should have 2 schemas now
        assertThat(evolvedMetadata.currentSchemaId())
                .isEqualTo(1); // Current schema should be the new one

        // Verify the current schema has the new field
        IcebergSchema currentSchema =
                evolvedMetadata.schemas().stream()
                        .filter(schema -> schema.schemaId() == evolvedMetadata.currentSchemaId())
                        .findFirst()
                        .orElseThrow();
        assertThat(currentSchema.fields()).hasSize(4);
        assertThat(currentSchema.fields().get(3).name()).isEqualTo("email");
        assertThat(currentSchema.fields().get(3).type()).isEqualTo("string");
    }

    @Test
    @DisplayName("Test reading metadata with Iceberg table partitioning evolution")
    void testReadMetadataWithIcebergPartitioningEvolution() throws Exception {
        // Create initial unpartitioned Iceberg table
        Table icebergTable = createBasicIcebergTable("partition_evolution_table");

        // Read initial metadata
        IcebergMetadata initialMetadata = readIcebergMetadata("partition_evolution_table");
        assertThat(initialMetadata.partitionSpecs()).hasSize(1);
        assertThat(initialMetadata.partitionSpecs().get(0).fields()).isEmpty(); // Unpartitioned

        // Evolve partitioning by adding a partition
        icebergTable.updateSpec().addField("name").commit();

        // Read metadata after partitioning evolution
        IcebergMetadata evolvedMetadata = readIcebergMetadata("partition_evolution_table");

        // Verify partitioning evolution
        assertThat(evolvedMetadata.partitionSpecs())
                .hasSize(2); // Should have 2 partition specs now
        assertThat(evolvedMetadata.defaultSpecId())
                .isEqualTo(1); // Default spec should be the new one

        // Verify the current partition spec has the new field
        IcebergPartitionSpec currentSpec =
                evolvedMetadata.partitionSpecs().stream()
                        .filter(spec -> spec.specId() == evolvedMetadata.defaultSpecId())
                        .findFirst()
                        .orElseThrow();
        assertThat(currentSpec.fields()).hasSize(1);
        assertThat(currentSpec.fields().get(0).name()).isEqualTo("name");
        assertThat(currentSpec.fields().get(0).transform()).isEqualTo("identity");
    }

    @Test
    @DisplayName("Test FORMAT_VERSION_V3 table")
    void testFormatVersionV3Table() throws Exception {
        // Create a v3 format version Iceberg table
        Table icebergTable = createIcebergTableV3("v3_snapshot_table");
        TableMetadata base = ((HasTableOperations) icebergTable).operations().current();
        ((HasTableOperations) icebergTable)
                .operations()
                .commit(base, TableMetadata.buildFrom(base).enableRowLineage().build());

        // Read metadata using Paimon's IcebergMetadata
        IcebergMetadata paimonIcebergMetadata = readIcebergMetadata(icebergTable);

        // Verify basic properties
        assertThat(paimonIcebergMetadata.formatVersion()).isEqualTo(3);
        assertThat(paimonIcebergMetadata.tableUuid()).isNotNull();
        assertThat(paimonIcebergMetadata.location()).isNotNull();
        assertThat(paimonIcebergMetadata.currentSchemaId()).isEqualTo(0);
        assertThat(paimonIcebergMetadata.defaultSpecId()).isEqualTo(0);
        assertThat(paimonIcebergMetadata.defaultSortOrderId()).isEqualTo(0);

        // Verify schema
        assertThat(paimonIcebergMetadata.schemas()).hasSize(1);
        IcebergSchema schema = paimonIcebergMetadata.schemas().get(0);
        assertThat(schema.fields()).hasSize(3);

        // Verify field details
        assertThat(schema.fields().get(0).name()).isEqualTo("id");
        assertThat(schema.fields().get(0).type()).isEqualTo("int");
        assertThat(schema.fields().get(0).required()).isTrue();

        assertThat(schema.fields().get(1).name()).isEqualTo("name");
        assertThat(schema.fields().get(1).type()).isEqualTo("string");
        assertThat(schema.fields().get(1).required()).isTrue();

        assertThat(schema.fields().get(2).name()).isEqualTo("age");
        assertThat(schema.fields().get(2).type()).isEqualTo("int");
        assertThat(schema.fields().get(2).required()).isFalse();

        // Verify partition specs (should be unpartitioned)
        assertThat(paimonIcebergMetadata.partitionSpecs()).hasSize(1);
        assertThat(paimonIcebergMetadata.partitionSpecs().get(0).fields()).isEmpty();

        // Perform first append operation
        icebergTable
                .newFastAppend()
                .appendFile(
                        DataFiles.builder(PartitionSpec.unpartitioned())
                                .withPath("/path/to/data-v3.parquet")
                                .withFileSizeInBytes(100)
                                .withRecordCount(10)
                                .build())
                .commit();

        // Read metadata after first operation
        paimonIcebergMetadata = readIcebergMetadata("v3_snapshot_table");

        // Verify snapshots after first operation
        assertThat(paimonIcebergMetadata.snapshots()).hasSize(1);
        assertThat(paimonIcebergMetadata.currentSnapshotId()).isNotNull();

        IcebergSnapshot snapshot = paimonIcebergMetadata.snapshots().get(0);
        assertThat(snapshot.parentSnapshotId()).isNull();

        assertThat(snapshot.firstRowId()).isEqualTo(0L);
        assertThat(snapshot.addedRows()).isEqualTo(10L);

        // Verify other snapshot properties
        assertThat(snapshot.snapshotId()).isNotNull();
        assertThat(snapshot.sequenceNumber()).isEqualTo(1L);
        assertThat(snapshot.timestampMs()).isGreaterThan(0);
        assertThat(snapshot.schemaId()).isEqualTo(0);

        // Perform second append operation
        icebergTable
                .newFastAppend()
                .appendFile(
                        DataFiles.builder(PartitionSpec.unpartitioned())
                                .withPath("/path/to/data-v3-2.parquet")
                                .withFileSizeInBytes(200)
                                .withRecordCount(20)
                                .build())
                .commit();

        // Read metadata after second operation
        IcebergMetadata paimonIcebergMetadata2 = readIcebergMetadata("v3_snapshot_table");

        // Verify second snapshot
        assertThat(paimonIcebergMetadata2.snapshots()).hasSize(2);
        assertThat(paimonIcebergMetadata2.currentSnapshotId())
                .isEqualTo(paimonIcebergMetadata2.snapshots().get(1).snapshotId());
        assertThat(paimonIcebergMetadata2.snapshots().get(1).parentSnapshotId())
                .isEqualTo(paimonIcebergMetadata2.snapshots().get(0).snapshotId());

        // Verify snapshot sequence numbers
        assertThat(paimonIcebergMetadata2.snapshots().get(0).sequenceNumber()).isEqualTo(1L);
        assertThat(paimonIcebergMetadata2.snapshots().get(1).sequenceNumber()).isEqualTo(2L);
    }

    /** Helper method to create a basic Iceberg table with simple schema. */
    private Table createBasicIcebergTable(String tableName) {
        TableIdentifier identifier = TableIdentifier.of("testdb", tableName);
        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "name", Types.StringType.get()),
                        Types.NestedField.optional(3, "age", Types.IntegerType.get()));
        return restCatalog.buildTable(identifier, schema).create();
    }

    /** Helper method to create an Iceberg table with partitioning. */
    private Table createPartitionedIcebergTable(String tableName) {
        TableIdentifier identifier = TableIdentifier.of("testdb", tableName);
        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "name", Types.StringType.get()),
                        Types.NestedField.required(3, "department", Types.StringType.get()),
                        Types.NestedField.required(4, "salary", Types.DoubleType.get()));
        PartitionSpec partitionSpec =
                PartitionSpec.builderFor(schema).identity("department").build();
        return restCatalog.buildTable(identifier, schema).withPartitionSpec(partitionSpec).create();
    }

    /** Helper method to create an Iceberg table with sort order. */
    private Table createSortedIcebergTable(String tableName) {
        TableIdentifier identifier = TableIdentifier.of("testdb", tableName);
        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "name", Types.StringType.get()),
                        Types.NestedField.required(3, "score", Types.DoubleType.get()));
        SortOrder sortOrder = SortOrder.builderFor(schema).asc("score").desc("id").build();
        return restCatalog.buildTable(identifier, schema).withSortOrder(sortOrder).create();
    }

    /** Helper method to create an Iceberg table with FORMAT_VERSION_V3. */
    private Table createIcebergTableV3(String tableName) {
        TableIdentifier identifier = TableIdentifier.of("testdb", tableName);
        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "name", Types.StringType.get()),
                        Types.NestedField.optional(3, "age", Types.IntegerType.get()));

        Map<String, String> properties = new HashMap<>();
        properties.put("write.format.default", "parquet");
        properties.put("write.parquet.compression-codec", "snappy");
        properties.put("format-version", "3");

        return restCatalog.buildTable(identifier, schema).withProperties(properties).create();
    }

    /** Helper method to read Iceberg metadata using Paimon's IcebergMetadata. */
    private IcebergMetadata readIcebergMetadata(String tableName) throws Exception {
        TableIdentifier identifier = TableIdentifier.of("testdb", tableName);
        Table icebergTable = restCatalog.loadTable(identifier);
        return readIcebergMetadata(icebergTable);
    }

    private IcebergMetadata readIcebergMetadata(Table icebergTable) throws Exception {
        String metaFileLocation = TableUtil.metadataFileLocation(icebergTable);
        Path metaFilePath = new Path(metaFileLocation);
        Options options = new Options();
        FileIO fileIO = FileIO.get(metaFilePath, CatalogContext.create(options));
        return IcebergMetadata.fromPath(fileIO, metaFilePath);
    }
}
