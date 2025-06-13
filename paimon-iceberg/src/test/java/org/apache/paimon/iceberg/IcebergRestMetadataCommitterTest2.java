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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogServer;
import org.apache.iceberg.rest.RESTServerExtension;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** doc. */
public class IcebergRestMetadataCommitterTest2 {

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

    @TempDir public java.nio.file.Path tempDir;

    protected static RESTCatalog restCatalog;

    @BeforeEach
    public void setUp() {
        restCatalog = REST_SERVER_EXTENSION.client();
    }

    @Test
    public void test() {
        // Define the namespace and table name
        Namespace namespace = Namespace.of("test_db1");
        TableIdentifier tableId = TableIdentifier.of(namespace, "rest_tbl1");

        // Check if the namespace exists, create it if it doesn't
        if (!restCatalog.namespaceExists(namespace)) {
            System.out.println("Creating namespace: " + namespace);
            restCatalog.createNamespace(namespace);
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
        String location = "/Users/catyeah/testHome/iceberg_rest";

        // Create the table
        System.out.println("Creating table: " + tableId);

        Table table =
                restCatalog
                        .buildTable(tableId, schema)
                        .withPartitionSpec(spec)
                        //                        .withLocation(location)
                        .withProperties(properties)
                        .create();

        System.out.println("Successfully created table: " + table.name());

        System.out.println();
    }

    @Test
    public void test2() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> customOptions = new HashMap<>();
        customOptions.put(
                IcebergOptions.REST_CONFIG_PREFIX + CatalogProperties.URI,
                restCatalog.properties().get(CatalogProperties.URI));
        customOptions.put(
                IcebergOptions.REST_CONFIG_PREFIX + CatalogProperties.WAREHOUSE_LOCATION,
                restCatalog.properties().get(CatalogProperties.WAREHOUSE_LOCATION));
        customOptions.put(
                IcebergOptions.REST_CONFIG_PREFIX + CatalogProperties.CLIENT_POOL_SIZE,
                restCatalog.properties().get(CatalogProperties.CLIENT_POOL_SIZE));

        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        customOptions);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(true, 1));

        Table icebergTable = restCatalog.loadTable(TableIdentifier.of("mydb", "t"));
        validateIcebergResult(
                icebergTable, Arrays.asList(new Object[] {1, 10}, new Object[] {2, 20}));

        write.write(GenericRow.of(3, 30));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));
        icebergTable = restCatalog.loadTable(TableIdentifier.of("mydb", "t"));
        validateIcebergResult(
                icebergTable,
                Arrays.asList(new Object[] {1, 10}, new Object[] {2, 20}, new Object[] {3, 30}));

        write.close();
        commit.close();
    }

    private FileStoreTable createPaimonTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            int numBuckets,
            Map<String, String> customOptions)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, numBuckets);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.REST_CATALOG);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 4);
        options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 8);
        options.set(IcebergOptions.METADATA_DELETE_AFTER_COMMIT, true);
        options.set(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX, 1);
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, MemorySize.ofKibiBytes(8));
        org.apache.paimon.schema.Schema schema =
                new org.apache.paimon.schema.Schema(
                        rowType.getFields(), partitionKeys, primaryKeys, options.toMap(), "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private void validateIcebergResult(Table icebergTable, List<Object[]> expected)
            throws Exception {
        Types.StructType type = icebergTable.schema().asStruct();

        StructLikeSet actualSet = StructLikeSet.create(type);
        StructLikeSet expectSet = StructLikeSet.create(type);

        try (CloseableIterable<Record> reader = IcebergGenerics.read(icebergTable).build()) {
            reader.forEach(actualSet::add);
        }
        expectSet.addAll(
                expected.stream().map(r -> icebergRecord(type, r)).collect(Collectors.toList()));

        assertThat(actualSet).isEqualTo(expectSet);
    }

    private org.apache.iceberg.data.GenericRecord icebergRecord(
            Types.StructType type, Object[] row) {
        org.apache.iceberg.data.GenericRecord record =
                org.apache.iceberg.data.GenericRecord.create(type);
        for (int i = 0; i < row.length; i++) {
            record.set(i, row[i]);
        }
        return record;
    }
}
