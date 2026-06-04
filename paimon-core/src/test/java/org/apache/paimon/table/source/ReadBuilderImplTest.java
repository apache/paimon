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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ReadBuilderImpl}. */
public class ReadBuilderImplTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testValidateSpecifiedBucketOnFixedBucketPrimaryKeyTable() throws Exception {
        FileStoreTable table = createTable("4", true);
        assertThatCode(() -> table.newReadBuilder().withBucket(0)).doesNotThrowAnyException();
    }

    @Test
    public void testValidateSpecifiedBucketRejectsDynamicBucketTable() throws Exception {
        FileStoreTable table = createTable("-1", true);
        assertThatThrownBy(() -> table.newReadBuilder().withBucket(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fixed-bucket tables")
                .hasMessageContaining("HASH_DYNAMIC");
    }

    @Test
    public void testValidateSpecifiedBucketRejectsPostponeBucketTable() throws Exception {
        FileStoreTable table = createTable("-2", true);
        assertThatThrownBy(() -> table.newReadBuilder().withBucket(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fixed-bucket tables")
                .hasMessageContaining("POSTPONE_MODE");
    }

    @Test
    public void testValidateSpecifiedBucketRejectsBucketUnawareTable() throws Exception {
        FileStoreTable table = createBucketUnawareAppendOnlyTable();
        assertThatThrownBy(() -> table.newReadBuilder().withBucket(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fixed-bucket tables")
                .hasMessageContaining("BUCKET_UNAWARE");
    }

    @Test
    public void testValidateSpecifiedBucketAcceptsAppendOnlyTable() throws Exception {
        FileStoreTable table = createAppendOnlyTable("4");
        assertThatCode(() -> table.newReadBuilder().withBucket(0)).doesNotThrowAnyException();
    }

    @Test
    public void testValidateSpecifiedBucketRejectsOutOfRangeBucketId() throws Exception {
        FileStoreTable table = createTable("4", true);
        assertThatThrownBy(() -> table.newReadBuilder().withBucket(4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Bucket id 4 must be less than table bucket number 4");
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

    private FileStoreTable createAppendOnlyTable(String bucket) throws Exception {
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

    private FileStoreTable createTable(Schema schema) throws Exception {
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        catalog.createDatabase("default", true);
        Identifier identifier = Identifier.create("default", "test_bucket");
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }
}
