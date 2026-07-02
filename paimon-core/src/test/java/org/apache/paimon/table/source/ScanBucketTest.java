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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CoreOptions#SCAN_BUCKET}. */
public class ScanBucketTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testWithBucketAllowsAppendOnlyFixedBucketTable() throws Exception {
        FileStoreTable table = createAppendOnlyFixedBucketTable("4");
        writeRows(table, 1, 2, 3, 4, 5, 6, 7, 8);

        assertThat(extractBuckets(table.newScan().withBucket(0).plan().splits()))
                .containsExactly(0);
        assertThat(extractBuckets(table.newReadBuilder().withBucket(0).newScan().plan().splits()))
                .containsExactly(0);
    }

    @Test
    public void testScanBucketOptionRejectsOutOfRangeBucketId() throws Exception {
        FileStoreTable table = createTableWithScanBucket("4", true, "5");
        assertThatThrownBy(() -> table.newScan().plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Bucket id 5 must be less than table bucket number 4");
    }

    @Test
    public void testScanBucketOptionRejectsDynamicBucketTable() throws Exception {
        FileStoreTable table = createTableWithScanBucket("-1", true, "0");
        assertThatThrownBy(() -> table.newScan().plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fixed-bucket tables");
    }

    @Test
    public void testScanBucketOptionRejectsPostponeBucketTable() throws Exception {
        FileStoreTable table = createTableWithScanBucket("-2", true, "0");
        assertThatThrownBy(() -> table.newScan().plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fixed-bucket tables");
    }

    @Test
    public void testScanBucketOptionRejectsBucketUnawareTable() throws Exception {
        FileStoreTable table = createBucketUnawareTableWithScanBucket("0");
        assertThatThrownBy(() -> table.newScan().plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary key tables");
    }

    @Test
    public void testScanBucketOptionRejectsTableWithoutPrimaryKey() throws Exception {
        FileStoreTable table = createAppendOnlyTableWithScanBucket("4", "0");
        assertThatThrownBy(() -> table.newScan().plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary key tables");
    }

    @Test
    public void testScanBucketOptionViaDirectTableScan() throws Exception {
        FileStoreTable table = createTable("4", true);
        writeRows(table, 1, 2, 3, 4, 5, 6, 7, 8);

        assertThat(extractBuckets(table.newScan().plan().splits()).size()).isGreaterThan(1);

        Map<String, String> options = new HashMap<>(table.options());
        options.put(CoreOptions.SCAN_BUCKET.key(), "0");
        FileStoreTable tableWithScanBucket = table.copy(options);
        assertThat(extractBuckets(tableWithScanBucket.newScan().plan().splits()))
                .containsExactly(0);
    }

    @Test
    public void testScanBucketOptionViaTableCopy() throws Exception {
        FileStoreTable table = createTable("4", true);
        writeRows(table, 1, 2, 3, 4, 5, 6, 7, 8);

        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCAN_BUCKET.key(), "0");
        FileStoreTable tableWithScanBucket = table.copy(options);

        assertThat(extractBuckets(table.newScan().plan().splits()).size()).isGreaterThan(1);
        assertThat(extractBuckets(tableWithScanBucket.newScan().plan().splits()))
                .containsExactly(0);
    }

    @Test
    public void testScanBucketOptionViaReadBuilder() throws Exception {
        FileStoreTable table = createTableWithScanBucket("4", true, "0");
        writeRows(table, 1, 2, 3, 4, 5, 6, 7, 8);

        assertThat(extractBuckets(table.newReadBuilder().newScan().plan().splits()))
                .containsExactly(0);
    }

    @Test
    public void testScanBucketOptionRejectsDirectTableScanForDynamicBucketTable() throws Exception {
        FileStoreTable table = createTableWithScanBucket("-1", true, "0");
        assertThatThrownBy(() -> table.newScan().plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fixed-bucket tables");
    }

    private static List<Integer> extractBuckets(List<Split> splits) {
        return splits.stream()
                .map(split -> ((DataSplit) split).bucket())
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    private void writeRows(FileStoreTable table, int... ids) throws Exception {
        StreamTableWrite write = table.newWrite("test");
        StreamTableCommit commit = table.newCommit("test");
        for (int id : ids) {
            write.write(GenericRow.of(id, id));
        }
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();
    }

    private FileStoreTable createTableWithScanBucket(
            String bucket, boolean withPrimaryKey, String scanBucket) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), bucket);
        options.put(CoreOptions.SCAN_BUCKET.key(), scanBucket);
        Schema.Builder schemaBuilder =
                Schema.newBuilder().column("id", DataTypes.INT()).column("v", DataTypes.INT());
        if (withPrimaryKey) {
            schemaBuilder.primaryKey("id");
        }
        Schema schema = schemaBuilder.options(options).build();
        return createTable(schema);
    }

    private FileStoreTable createBucketUnawareTableWithScanBucket(String scanBucket)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.SCAN_BUCKET.key(), scanBucket);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .options(options)
                        .build();
        return createTable(schema);
    }

    private FileStoreTable createAppendOnlyFixedBucketTable(String bucket) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), bucket);
        options.put(CoreOptions.BUCKET_KEY.key(), "id");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .options(options)
                        .build();
        return createTable(schema);
    }

    private FileStoreTable createAppendOnlyTableWithScanBucket(String bucket, String scanBucket)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), bucket);
        options.put(CoreOptions.BUCKET_KEY.key(), "id");
        options.put(CoreOptions.SCAN_BUCKET.key(), scanBucket);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .options(options)
                        .build();
        return createTable(schema);
    }

    private FileStoreTable createTable(String bucket, boolean withPrimaryKey) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), bucket);
        Schema.Builder schemaBuilder =
                Schema.newBuilder().column("id", DataTypes.INT()).column("v", DataTypes.INT());
        if (withPrimaryKey) {
            schemaBuilder.primaryKey("id");
        }
        Schema schema = schemaBuilder.options(options).build();
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
