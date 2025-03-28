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

package org.apache.paimon.iceberg.migrate;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Test for {@link IcebergMigrateHadoopMetadata}. */
class IcebergMigrateHadoopMetadataTest {
    @TempDir java.nio.file.Path iceTempDir;

    Schema iceSchema =
            new Schema(
                    Types.NestedField.required(1, "k", Types.IntegerType.get()),
                    Types.NestedField.required(2, "v", Types.IntegerType.get()),
                    Types.NestedField.required(3, "dt", Types.StringType.get()),
                    Types.NestedField.required(4, "hh", Types.StringType.get()));

    @Test
    void testGetIcebergMetadataWithCustomFileIO() {
        createIcebergTable();

        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "hadoop-catalog");
        icebergProperties.put(
                "iceberg_warehouse", TestFileIOLoader.SCHEME + "://" + iceTempDir.toString());

        IcebergMigrateHadoopMetadata icebergMigrateHadoopMetadata =
                new IcebergMigrateHadoopMetadata(
                        Identifier.create("ice_db", "ice_t"), new Options(icebergProperties));

        assertDoesNotThrow(icebergMigrateHadoopMetadata::icebergMetadata);
    }

    private Table createIcebergTable() {

        Map<String, String> icebergCatalogOptions = new HashMap<>();
        icebergCatalogOptions.put("type", "hadoop");
        icebergCatalogOptions.put("warehouse", iceTempDir.toString());

        org.apache.iceberg.catalog.Catalog icebergCatalog =
                CatalogUtil.buildIcebergCatalog(
                        "iceberg_catalog", icebergCatalogOptions, new Configuration());
        TableIdentifier icebergIdentifier = TableIdentifier.of("ice_db", "ice_t");

        return icebergCatalog
                .buildTable(icebergIdentifier, iceSchema)
                .withPartitionSpec(PartitionSpec.unpartitioned())
                .create();
    }

    /** {@link FileIOLoader} for this test. */
    public static class TestFileIOLoader implements FileIOLoader {

        private static final long serialVersionUID = 1L;

        private static final String SCHEME = "iceberg-test-file-io";

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileIO load(Path path) {
            return LocalFileIO.create();
        }
    }
}
