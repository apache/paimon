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

package org.apache.paimon.flink.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ScanBucketUtils}. */
public class ScanBucketUtilsTest extends AbstractTestBase {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testInvalidBucket() throws Exception {
        FileStoreTable table = createPrimaryKeyTable("4");
        Options options = new Options();
        options.set(FlinkConnectorOptions.SCAN_BUCKET, 5);
        ReadBuilder readBuilder = table.newReadBuilder();
        assertThatThrownBy(() -> ScanBucketUtils.applyScanBucket(table, readBuilder, options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Bucket id 5 must be less than table bucket number 4");
    }

    @Test
    public void testRejectDynamicBucketTable() throws Exception {
        FileStoreTable table = createPrimaryKeyTable("-1");
        Options options = new Options();
        options.set(FlinkConnectorOptions.SCAN_BUCKET, 0);
        ReadBuilder readBuilder = table.newReadBuilder();
        assertThatThrownBy(() -> ScanBucketUtils.applyScanBucket(table, readBuilder, options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fixed-bucket tables");
    }

    @Test
    public void testRejectBucketUnawareTable() throws Exception {
        FileStoreTable table = createBucketUnawareAppendOnlyTable();
        Options options = new Options();
        options.set(FlinkConnectorOptions.SCAN_BUCKET, 0);
        ReadBuilder readBuilder = table.newReadBuilder();
        assertThatThrownBy(() -> ScanBucketUtils.applyScanBucket(table, readBuilder, options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary key tables");
    }

    @Test
    public void testRejectTableWithoutPrimaryKey() throws Exception {
        FileStoreTable table = createAppendOnlyTable("4");
        Options options = new Options();
        options.set(FlinkConnectorOptions.SCAN_BUCKET, 0);
        ReadBuilder readBuilder = table.newReadBuilder();
        assertThatThrownBy(() -> ScanBucketUtils.applyScanBucket(table, readBuilder, options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary key tables");
    }

    private FileStoreTable createPrimaryKeyTable(String numBuckets) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), numBuckets);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("id")
                        .options(options)
                        .build();
        return createTable(schema);
    }

    private FileStoreTable createBucketUnawareAppendOnlyTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .options(options)
                        .build();
        return createTable(schema);
    }

    private FileStoreTable createAppendOnlyTable(String numBuckets) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), numBuckets);
        options.put(CoreOptions.BUCKET_KEY.key(), "id");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .options(options)
                        .build();
        return createTable(schema);
    }

    private FileStoreTable createTable(Schema schema) throws Exception {
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        catalog.createDatabase("default", true);
        Identifier identifier = Identifier.create("default", "test_bucket");
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }
}
