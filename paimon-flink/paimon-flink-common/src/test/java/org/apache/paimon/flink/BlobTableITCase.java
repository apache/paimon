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
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.BlobViewStruct;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.TestHttpWebServer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.UriReader;
import org.apache.paimon.utils.UriReaderFactory;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test write and read table with blob type. */
public class BlobTableITCase extends CatalogITCaseBase {

    private static final Random RANDOM = new Random();
    @TempDir private Path warehouse;

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS blob_table (id INT, data STRING, picture BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-compaction.enabled'='true', 'blob-field'='picture')",
                "CREATE TABLE IF NOT EXISTS blob_table_descriptor (id INT, data STRING, picture BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture', 'blob-as-descriptor'='true')",
                "CREATE TABLE IF NOT EXISTS multiple_blob_table (id INT, data STRING, pic1 BYTES, pic2 BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-compaction.enabled'='true', 'blob-field'='pic1,pic2')",
                "CREATE TABLE IF NOT EXISTS array_blob_table (id INT, data STRING, pictures ARRAY<BYTES>) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-compaction.enabled'='true', 'blob-field'='pictures')",
                "CREATE TABLE IF NOT EXISTS mixed_blob_table"
                        + " (id INT, data STRING, raw_pic BYTES, desc_pic BYTES)"
                        + " WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true',"
                        + " 'blob-field'='raw_pic,desc_pic', 'blob-descriptor-field'='desc_pic')");
    }

    @Test
    public void testBasic() {
        batchSql("SELECT * FROM blob_table");
        batchSql("INSERT INTO blob_table VALUES (1, 'paimon', X'48656C6C6F')");
        assertThat(batchSql("SELECT * FROM blob_table"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "paimon", new byte[] {72, 101, 108, 108, 111}));
        assertThat(batchSql("SELECT picture FROM blob_table"))
                .containsExactlyInAnyOrder(Row.of(new byte[] {72, 101, 108, 108, 111}));
        assertThat(batchSql("SELECT file_path FROM `blob_table$files`").size()).isEqualTo(2);
    }

    @Test
    public void testMultipleBlobs() {
        batchSql("SELECT * FROM multiple_blob_table");
        batchSql("INSERT INTO multiple_blob_table VALUES (1, 'paimon', X'48656C6C6F', X'5945')");
        assertThat(batchSql("SELECT * FROM multiple_blob_table"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                "paimon",
                                new byte[] {72, 101, 108, 108, 111},
                                new byte[] {89, 69}));
        assertThat(batchSql("SELECT pic1 FROM multiple_blob_table"))
                .containsExactlyInAnyOrder(Row.of(new byte[] {72, 101, 108, 108, 111}));
        assertThat(batchSql("SELECT pic2 FROM multiple_blob_table"))
                .containsExactlyInAnyOrder(Row.of(new byte[] {89, 69}));
        assertThat(batchSql("SELECT file_path FROM `multiple_blob_table$files`").size())
                .isEqualTo(3);
    }

    @Test
    public void testArrayBlobField() throws Exception {
        org.apache.paimon.types.ArrayType arrayType =
                (org.apache.paimon.types.ArrayType)
                        paimonTable("array_blob_table").rowType().getTypeAt(2);
        assertThat(arrayType.getElementType().getTypeRoot()).isEqualTo(DataTypeRoot.BLOB);

        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(
                                        1,
                                        "paimon",
                                        new byte[][] {
                                            new byte[] {72, 101, 108, 108, 111},
                                            null,
                                            new byte[] {89, 69}
                                        }),
                                Row.of(2, "one", new byte[][] {new byte[] {65, 66, 67}}),
                                Row.of(3, "null-array", null)));
        tEnv.executeSql(
                        String.format(
                                "CREATE TEMPORARY TABLE array_blob_source "
                                        + "(id INT, data STRING, pictures ARRAY<BYTES>) "
                                        + "WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')",
                                dataId))
                .await();
        batchSql("INSERT INTO array_blob_table SELECT * FROM array_blob_source");

        assertThat(
                        batchSql(
                                "SELECT id, CARDINALITY(pictures), pictures[1], pictures[2], pictures[3] "
                                        + "FROM array_blob_table ORDER BY id"))
                .containsExactly(
                        Row.of(
                                1,
                                3,
                                new byte[] {72, 101, 108, 108, 111},
                                null,
                                new byte[] {89, 69}),
                        Row.of(2, 1, new byte[] {65, 66, 67}, null, null),
                        Row.of(3, null, null, null, null));
        assertThat(
                        batchSql(
                                "SELECT COUNT(*) > 0 FROM `array_blob_table$files` WHERE file_path LIKE '%%.blob'"))
                .containsExactly(Row.of(true));
    }

    @Test
    public void testPrimaryKeyArrayBlobField() throws Exception {
        batchSql(
                "CREATE TABLE pk_array_blob_table ("
                        + "id INT, pictures ARRAY<BYTES>, "
                        + "PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ('bucket'='1', 'blob-field'='pictures')");
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(
                                        1,
                                        new byte[][] {
                                            new byte[] {72, 101, 108, 108, 111},
                                            null,
                                            new byte[] {89, 69}
                                        })));
        tEnv.executeSql(
                        String.format(
                                "CREATE TEMPORARY TABLE pk_array_blob_source "
                                        + "(id INT, pictures ARRAY<BYTES>) "
                                        + "WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')",
                                dataId))
                .await();

        batchSql("INSERT INTO pk_array_blob_table SELECT * FROM pk_array_blob_source");

        assertThat(
                        batchSql(
                                "SELECT id, CARDINALITY(pictures), pictures[1], pictures[2], pictures[3] "
                                        + "FROM pk_array_blob_table"))
                .containsExactly(
                        Row.of(
                                1,
                                3,
                                new byte[] {72, 101, 108, 108, 111},
                                null,
                                new byte[] {89, 69}));
    }

    @Test
    public void testArrayBlobFieldWithDescriptorElements() throws Exception {
        byte[] blobData = new byte[1024];
        RANDOM.nextBytes(blobData);
        FileIO fileIO = new LocalFileIO();
        String uri = "file://" + warehouse + "/external_array_blob";
        try (OutputStream outputStream =
                fileIO.newOutputStream(new org.apache.paimon.fs.Path(uri), true)) {
            outputStream.write(blobData);
        }

        tEnv.executeSql(
                        "CREATE TABLE array_blob_descriptor_table "
                                + "(id INT, data STRING, pictures ARRAY<BYTES>) "
                                + "WITH ('row-tracking.enabled'='true', "
                                + "'data-evolution.enabled'='true', "
                                + "'blob-field'='pictures', "
                                + "'blob-as-descriptor'='true')")
                .await();
        batchSql(
                "INSERT INTO array_blob_descriptor_table VALUES "
                        + "(1, 'paimon', ARRAY[sys.path_to_descriptor('"
                        + uri
                        + "'), X'5945'])");

        Row descriptorRow =
                batchSql("SELECT pictures[1], pictures[2] " + "FROM array_blob_descriptor_table")
                        .get(0);
        BlobDescriptor firstDescriptor =
                BlobDescriptor.deserialize((byte[]) descriptorRow.getField(0));
        BlobDescriptor secondDescriptor =
                BlobDescriptor.deserialize((byte[]) descriptorRow.getField(1));
        Options options = new Options();
        options.set("warehouse", warehouse.toString());
        CatalogContext catalogContext = CatalogContext.create(options);
        UriReaderFactory uriReaderFactory = new UriReaderFactory(catalogContext);
        Blob firstBlob =
                Blob.fromDescriptor(
                        uriReaderFactory.create(firstDescriptor.uri()), firstDescriptor);
        Blob secondBlob =
                Blob.fromDescriptor(
                        uriReaderFactory.create(secondDescriptor.uri()), secondDescriptor);
        assertThat(firstBlob.toData()).isEqualTo(blobData);
        assertThat(secondBlob.toData()).isEqualTo(new byte[] {89, 69});

        batchSql("ALTER TABLE array_blob_descriptor_table SET ('blob-as-descriptor'='false')");
        assertThat(
                        batchSql(
                                "SELECT pictures[1], pictures[2] "
                                        + "FROM array_blob_descriptor_table"))
                .containsExactly(Row.of(blobData, new byte[] {89, 69}));
    }

    @Test
    public void testBlobCompaction() throws Exception {
        for (int i = 1; i <= 10; i++) {
            batchSql("INSERT INTO blob_table VALUES (%s, 'paimon', X'48656C6C6F')", i);
        }
        batchSql("INSERT INTO blob_table VALUES (1, 'paimon', X'48656C6C6F')");

        assertThat(batchSql("SELECT COUNT(*) FROM `blob_table$files`"))
                .containsExactly(Row.of(22L));
        assertThat(
                        batchSql(
                                "SELECT COUNT(*) FROM `blob_table$files` WHERE file_path LIKE '%%.blob'"))
                .containsExactly(Row.of(11L));
        assertThat(
                        batchSql(
                                "SELECT COUNT(*) FROM `blob_table$files` WHERE file_path NOT LIKE '%%.blob'"))
                .containsExactly(Row.of(11L));

        tEnv.getConfig().set("table.dml-sync", "true");
        tEnv.executeSql("CALL sys.compact(`table` => 'default.blob_table')").await();

        assertThat(batchSql("SELECT COUNT(*) FROM `blob_table$files`")).containsExactly(Row.of(2L));
        assertThat(
                        batchSql(
                                "SELECT COUNT(*) FROM `blob_table$files` WHERE file_path LIKE '%%.blob'"))
                .containsExactly(Row.of(1L));
        assertThat(
                        batchSql(
                                "SELECT COUNT(*) FROM `blob_table$files` WHERE file_path NOT LIKE '%%.blob'"))
                .containsExactly(Row.of(1L));
        assertThat(batchSql("SELECT COUNT(*) FROM blob_table")).containsExactly(Row.of(11L));
        assertThat(batchSql("SELECT picture FROM blob_table WHERE id = 1"))
                .containsExactlyInAnyOrder(
                        Row.of(new byte[] {72, 101, 108, 108, 111}),
                        Row.of(new byte[] {72, 101, 108, 108, 111}));
    }

    @Test
    public void testMultipleBlobCompaction() throws Exception {
        for (int i = 1; i <= 10; i++) {
            batchSql(
                    "INSERT INTO multiple_blob_table VALUES (%s, 'paimon', X'48656C6C6F', X'5945')",
                    i);
        }
        batchSql("INSERT INTO multiple_blob_table VALUES (1, 'paimon', X'48656C6C6F', X'5945')");

        assertThat(batchSql("SELECT COUNT(*) FROM `multiple_blob_table$files`"))
                .containsExactly(Row.of(33L));
        assertThat(
                        batchSql(
                                "SELECT COUNT(*) FROM `multiple_blob_table$files` WHERE file_path LIKE '%%.blob'"))
                .containsExactly(Row.of(22L));
        assertThat(
                        batchSql(
                                "SELECT COUNT(*) FROM `multiple_blob_table$files` WHERE file_path NOT LIKE '%%.blob'"))
                .containsExactly(Row.of(11L));

        tEnv.getConfig().set("table.dml-sync", "true");
        tEnv.executeSql("CALL sys.compact(`table` => 'default.multiple_blob_table')").await();

        assertThat(batchSql("SELECT COUNT(*) FROM `multiple_blob_table$files`"))
                .containsExactly(Row.of(3L));
        assertThat(
                        batchSql(
                                "SELECT COUNT(*) FROM `multiple_blob_table$files` WHERE file_path LIKE '%%.blob'"))
                .containsExactly(Row.of(2L));
        assertThat(
                        batchSql(
                                "SELECT COUNT(*) FROM `multiple_blob_table$files` WHERE file_path NOT LIKE '%%.blob'"))
                .containsExactly(Row.of(1L));
        assertThat(batchSql("SELECT COUNT(*) FROM multiple_blob_table"))
                .containsExactly(Row.of(11L));
        assertThat(batchSql("SELECT pic1, pic2 FROM multiple_blob_table WHERE id = 1"))
                .containsExactlyInAnyOrder(
                        Row.of(new byte[] {72, 101, 108, 108, 111}, new byte[] {89, 69}),
                        Row.of(new byte[] {72, 101, 108, 108, 111}, new byte[] {89, 69}));
    }

    @Test
    public void testBlobCompactionDisabledByDefault() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE blob_compaction_disabled (id INT, data STRING, picture BYTES) "
                        + "WITH ('row-tracking.enabled'='true', "
                        + "'data-evolution.enabled'='true', "
                        + "'blob-field'='picture')");

        for (int i = 1; i <= 10; i++) {
            batchSql(
                    "INSERT INTO blob_compaction_disabled VALUES (%s, 'paimon', X'48656C6C6F')", i);
        }
        batchSql("INSERT INTO blob_compaction_disabled VALUES (1, 'paimon', X'48656C6C6F')");

        assertThat(
                        batchSql(
                                "SELECT COUNT(*) FROM `blob_compaction_disabled$files` WHERE file_path LIKE '%%.blob'"))
                .containsExactly(Row.of(11L));

        tEnv.getConfig().set("table.dml-sync", "true");
        tEnv.executeSql("CALL sys.compact(`table` => 'default.blob_compaction_disabled')").await();

        assertThat(
                        batchSql(
                                "SELECT COUNT(*) FROM `blob_compaction_disabled$files` WHERE file_path LIKE '%%.blob'"))
                .containsExactly(Row.of(11L));
        assertThat(
                        batchSql(
                                "SELECT COUNT(*) FROM `blob_compaction_disabled$files` WHERE file_path NOT LIKE '%%.blob'"))
                .containsExactly(Row.of(1L));
        assertThat(batchSql("SELECT COUNT(*) FROM blob_compaction_disabled"))
                .containsExactly(Row.of(11L));
    }

    @Test
    public void testWriteBlobAsDescriptor() throws Exception {
        byte[] blobData = new byte[1024 * 1024];
        RANDOM.nextBytes(blobData);
        FileIO fileIO = new LocalFileIO();
        String uri = "file://" + warehouse + "/external_blob";
        try (OutputStream outputStream =
                fileIO.newOutputStream(new org.apache.paimon.fs.Path(uri), true)) {
            outputStream.write(blobData);
        }

        BlobDescriptor blobDescriptor = new BlobDescriptor(uri, 0, blobData.length);
        batchSql(
                "INSERT INTO blob_table_descriptor VALUES (1, 'paimon', X'"
                        + bytesToHex(blobDescriptor.serialize())
                        + "')");
        byte[] newDescriptorBytes =
                (byte[]) batchSql("SELECT picture FROM blob_table_descriptor").get(0).getField(0);
        BlobDescriptor newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes);
        Options options = new Options();
        options.set("warehouse", warehouse.toString());
        CatalogContext catalogContext = CatalogContext.create(options);
        UriReaderFactory uriReaderFactory = new UriReaderFactory(catalogContext);
        Blob blob =
                Blob.fromDescriptor(
                        uriReaderFactory.create(newBlobDescriptor.uri()), blobDescriptor);
        assertThat(blob.toData()).isEqualTo(blobData);
        URI blobUri = URI.create(blob.toDescriptor().uri());
        assertThat(blobUri.getScheme()).isNotNull();
        batchSql("ALTER TABLE blob_table_descriptor SET ('blob-as-descriptor'='false')");
        assertThat(batchSql("SELECT * FROM blob_table_descriptor"))
                .containsExactlyInAnyOrder(Row.of(1, "paimon", blobData));
    }

    @Test
    public void testMaterializeDescriptorWithSourceTableFileIO() {
        tEnv.executeSql(
                "CREATE TABLE blob_descriptor_source (id INT, picture BYTES)"
                        + " WITH ('row-tracking.enabled'='true',"
                        + " 'data-evolution.enabled'='true',"
                        + " 'blob-field'='picture')");
        batchSql("INSERT INTO blob_descriptor_source VALUES" + " (1, X'48656C6C6F'), (2, X'5945')");
        batchSql("ALTER TABLE blob_descriptor_source SET ('blob-as-descriptor'='true')");

        String sourceTable = tEnv.getCurrentDatabase() + ".blob_descriptor_source";
        tEnv.executeSql(
                "CREATE TABLE blob_descriptor_target ("
                        + "id INT, picture BYTES, PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('bucket'='2',"
                        + " 'blob-field'='picture',"
                        + " 'blob-descriptor.source-table'='"
                        + sourceTable
                        + "')");
        batchSql(
                "INSERT INTO blob_descriptor_target "
                        + "/*+ OPTIONS('sink.parallelism' = '2') */ "
                        + "SELECT * FROM blob_descriptor_source");

        assertThat(batchSql("SELECT * FROM blob_descriptor_target ORDER BY id"))
                .containsExactly(
                        Row.of(1, new byte[] {72, 101, 108, 108, 111}),
                        Row.of(2, new byte[] {89, 69}));
    }

    @Test
    public void testWriteBlobWithBuiltInFunction() throws Exception {
        byte[] blobData = new byte[1024 * 1024];
        RANDOM.nextBytes(blobData);
        FileIO fileIO = new LocalFileIO();
        String uri = "file://" + warehouse + "/external_blob";
        try (OutputStream outputStream =
                fileIO.newOutputStream(new org.apache.paimon.fs.Path(uri), true)) {
            outputStream.write(blobData);
        }

        BlobDescriptor blobDescriptor = new BlobDescriptor(uri, 0, blobData.length);
        batchSql(
                "INSERT INTO blob_table_descriptor VALUES (1, 'paimon', sys.path_to_descriptor('"
                        + uri
                        + "'))");
        byte[] newDescriptorBytes =
                (byte[]) batchSql("SELECT picture FROM blob_table_descriptor").get(0).getField(0);
        BlobDescriptor newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes);
        Options options = new Options();
        options.set("warehouse", warehouse.toString());
        CatalogContext catalogContext = CatalogContext.create(options);
        UriReaderFactory uriReaderFactory = new UriReaderFactory(catalogContext);
        Blob blob =
                Blob.fromDescriptor(
                        uriReaderFactory.create(newBlobDescriptor.uri()), blobDescriptor);
        assertThat(blob.toData()).isEqualTo(blobData);
        URI blobUri = URI.create(blob.toDescriptor().uri());
        assertThat(blobUri.getScheme()).isNotNull();
        batchSql("ALTER TABLE blob_table_descriptor SET ('blob-as-descriptor'='false')");
        assertThat(batchSql("SELECT * FROM blob_table_descriptor"))
                .containsExactlyInAnyOrder(Row.of(1, "paimon", blobData));
    }

    @Test
    public void testWriteBlobViewWithBuiltInFunction() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE upstream_blob_view (id INT, name STRING, picture BYTES)"
                        + " WITH ('row-tracking.enabled'='true',"
                        + " 'data-evolution.enabled'='true',"
                        + " 'blob-field'='picture')");
        batchSql("INSERT INTO upstream_blob_view VALUES (1, 'row1', X'48656C6C6F')");
        batchSql("INSERT INTO upstream_blob_view VALUES (2, 'row2', X'5945')");

        String fullTableName = tEnv.getCurrentDatabase() + ".upstream_blob_view";

        tEnv.executeSql(
                "CREATE TABLE downstream_blob_view (id INT, label STRING, image_ref BYTES)"
                        + " WITH ('row-tracking.enabled'='true',"
                        + " 'data-evolution.enabled'='true',"
                        + " 'blob-view-field'='image_ref')");

        batchSql(
                String.format(
                        "INSERT INTO downstream_blob_view"
                                + " SELECT id, name, sys.blob_view('%s', 'picture', _ROW_ID)"
                                + " FROM `upstream_blob_view$row_tracking`",
                        fullTableName));

        List<Row> result = batchSql("SELECT * FROM downstream_blob_view ORDER BY id");
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getField(0)).isEqualTo(1);
        assertThat(result.get(0).getField(1)).isEqualTo("row1");
        assertThat((byte[]) result.get(0).getField(2))
                .isEqualTo(new byte[] {72, 101, 108, 108, 111});
        assertThat(result.get(1).getField(0)).isEqualTo(2);
        assertThat(result.get(1).getField(1)).isEqualTo("row2");
        assertThat((byte[]) result.get(1).getField(2)).isEqualTo(new byte[] {89, 69});
    }

    @Test
    public void testForwardBlobViewReferenceWithDynamicOption() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE upstream_blob_view_forward (id INT, name STRING, picture BYTES)"
                        + " WITH ('row-tracking.enabled'='true',"
                        + " 'data-evolution.enabled'='true',"
                        + " 'blob-field'='picture')");
        batchSql("INSERT INTO upstream_blob_view_forward VALUES (1, 'row1', X'48656C6C6F')");
        batchSql("INSERT INTO upstream_blob_view_forward VALUES (2, 'row2', X'5945')");

        String upstreamFullTableName = tEnv.getCurrentDatabase() + ".upstream_blob_view_forward";
        tEnv.executeSql(
                "CREATE TABLE first_downstream_blob_view (id INT, label STRING, image_ref BYTES)"
                        + " WITH ('row-tracking.enabled'='true',"
                        + " 'data-evolution.enabled'='true',"
                        + " 'blob-view-field'='image_ref')");
        batchSql(
                String.format(
                        "INSERT INTO first_downstream_blob_view"
                                + " SELECT id, name, sys.blob_view('%s', 'picture', _ROW_ID)"
                                + " FROM `upstream_blob_view_forward$row_tracking`",
                        upstreamFullTableName));

        tEnv.executeSql(
                "CREATE TABLE second_downstream_blob_view (id INT, label STRING, image_ref BYTES)"
                        + " WITH ('row-tracking.enabled'='true',"
                        + " 'data-evolution.enabled'='true',"
                        + " 'blob-view-field'='image_ref')");
        batchSql(
                "INSERT INTO second_downstream_blob_view"
                        + " SELECT id, label, image_ref"
                        + " FROM first_downstream_blob_view"
                        + " /*+ OPTIONS('blob-view.resolve.enabled'='false') */");

        assertThat(batchSql("SELECT * FROM second_downstream_blob_view ORDER BY id"))
                .containsExactly(
                        Row.of(1, "row1", new byte[] {72, 101, 108, 108, 111}),
                        Row.of(2, "row2", new byte[] {89, 69}));

        List<Row> originalReferences =
                batchSql(
                        "SELECT image_ref"
                                + " FROM first_downstream_blob_view"
                                + " /*+ OPTIONS('blob-view.resolve.enabled'='false') */"
                                + " ORDER BY id");
        List<Row> forwardedReferences =
                batchSql(
                        "SELECT image_ref"
                                + " FROM second_downstream_blob_view"
                                + " /*+ OPTIONS('blob-view.resolve.enabled'='false') */"
                                + " ORDER BY id");

        assertThat(forwardedReferences).hasSize(originalReferences.size());
        for (int i = 0; i < forwardedReferences.size(); i++) {
            byte[] originalReference = (byte[]) originalReferences.get(i).getField(0);
            byte[] forwardedReference = (byte[]) forwardedReferences.get(i).getField(0);
            assertThat(forwardedReference).isEqualTo(originalReference);
            assertThat(BlobViewStruct.deserialize(forwardedReference).identifier().getFullName())
                    .isEqualTo(upstreamFullTableName);
        }
    }

    @Test
    public void testBlobViewRejectsUnqualifiedTableName() {
        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "SELECT sys.blob_view("
                                                + "'upstream_blob_view', "
                                                + "'picture', "
                                                + "CAST(0 AS BIGINT))"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Table name must be 'database.table' or 'PAIMON.database.table', "
                                + "but is 'upstream_blob_view'.");
    }

    @Test
    public void testBlobViewRejectsNonBlobField() {
        tEnv.executeSql(
                "CREATE TABLE upstream_non_blob (id INT, picture BYTES)"
                        + " WITH ('row-tracking.enabled'='true',"
                        + " 'data-evolution.enabled'='true')");

        String fullTableName = tEnv.getCurrentDatabase() + ".upstream_non_blob";
        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "SELECT sys.blob_view("
                                                + "'%s', "
                                                + "'picture', "
                                                + "CAST(0 AS BIGINT))",
                                        fullTableName))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Field picture in upstream table "
                                + fullTableName
                                + " is not a BLOB field.");
    }

    @Test
    public void testBlobInlineFieldCanDeclareBlobWithoutBlobField() throws Exception {
        assertSecondaryBlobFieldCanDeclareBlobWithoutBlobField(
                "blob_descriptor_without_blob_field", "blob-descriptor-field");
        assertSecondaryBlobFieldCanDeclareBlobWithoutBlobField(
                "blob_view_without_blob_field", "blob-view-field");
    }

    @Test
    public void testBlobInlineFieldRejectsArrayBlob() {
        assertArrayBlobInlineFieldRejected(
                "array_blob_descriptor_reject", "'blob-descriptor-field'='pictures'");
        assertArrayBlobInlineFieldRejected(
                "array_blob_view_reject", "'blob-view-field'='pictures'");
    }

    private void assertArrayBlobInlineFieldRejected(String tableName, String blobOptions) {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "CREATE TABLE %s (id INT, pictures ARRAY<BYTES>)"
                                                                + " WITH ('row-tracking.enabled'='true',"
                                                                + " 'data-evolution.enabled'='true', %s)",
                                                        tableName, blobOptions))
                                        .await())
                .hasRootCauseMessage("ARRAY<BLOB> is only supported by 'blob-field'.");
    }

    private void assertSecondaryBlobFieldCanDeclareBlobWithoutBlobField(
            String tableName, String optionKey) throws Exception {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s (id INT, picture BYTES)"
                                + " WITH ('row-tracking.enabled'='true',"
                                + " 'data-evolution.enabled'='true',"
                                + " '%s'='picture')",
                        tableName, optionKey));

        assertThat(paimonTable(tableName).rowType().getTypeAt(1).is(DataTypeRoot.BLOB)).isTrue();
    }

    @Test
    public void testRawAndDescriptorBlobCoexistence() throws Exception {
        // Prepare external blob for the descriptor field
        byte[] descBlobData = new byte[256];
        RANDOM.nextBytes(descBlobData);
        FileIO fileIO = new LocalFileIO();
        String uri = "file://" + warehouse + "/external_desc_blob";
        try (OutputStream outputStream =
                fileIO.newOutputStream(new org.apache.paimon.fs.Path(uri), true)) {
            outputStream.write(descBlobData);
        }

        BlobDescriptor descriptor = new BlobDescriptor(uri, 0, descBlobData.length);

        batchSql(
                String.format(
                        "INSERT INTO mixed_blob_table VALUES"
                                + " (1, 'mixed-types', X'48656C6C6F', X'%s')",
                        bytesToHex(descriptor.serialize())));

        // Read back and verify both blob storage modes
        List<Row> result = batchSql("SELECT id, data, raw_pic, desc_pic FROM mixed_blob_table");
        assertThat(result.size()).isEqualTo(1);
        Row row = result.get(0);
        assertThat(row.getField(0)).isEqualTo(1);
        assertThat(row.getField(1)).isEqualTo("mixed-types");
        assertThat((byte[]) row.getField(2)).isEqualTo(new byte[] {72, 101, 108, 108, 111});
        assertThat((byte[]) row.getField(3)).isEqualTo(descBlobData);
    }

    @Test
    public void testDynamicOptions() throws Exception {
        batchSql("INSERT INTO blob_table VALUES (1, 'paimon', X'48656C6C6F')");
        assertThat(batchSql("SELECT * FROM blob_table"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "paimon", new byte[] {72, 101, 108, 108, 111}));

        // set blob-as-descriptor to true
        tEnv.getConfig().set("paimon.*.*.blob_table.blob-as-descriptor", "true");
        List<Row> result = batchSql("SELECT * FROM blob_table$row_tracking");
        assertThat(result).size().isEqualTo(1);

        byte[] descriptorBytes = result.get(0).getFieldAs(2);
        BlobDescriptor descriptor = BlobDescriptor.deserialize(descriptorBytes);

        UriReader reader = UriReader.fromFile(LocalFileIO.INSTANCE);
        Blob blob = new BlobRef(reader, descriptor);
        assertThat(blob.toData()).isEqualTo(new byte[] {72, 101, 108, 108, 111});
    }

    @Test
    public void testRowTrackingWithBlobProjection() {
        batchSql("INSERT INTO blob_table VALUES (1, 'paimon', X'48656C6C6F')");

        // query _ROW_ID and blob field from row_tracking system table
        // this previously caused ArrayIndexOutOfBoundsException because BlobFormatReader
        // ignored the projectedRowType and always returned a single-field row
        List<Row> result = batchSql("SELECT _ROW_ID, picture FROM blob_table$row_tracking");
        assertThat(result).hasSize(1);
        Row row = result.get(0);
        assertThat(row.getField(0)).isNotNull();
        assertThat((byte[]) row.getField(1)).isEqualTo(new byte[] {72, 101, 108, 108, 111});

        // also verify selecting only _ROW_ID works
        List<Row> rowIdOnly = batchSql("SELECT _ROW_ID FROM blob_table$row_tracking");
        assertThat(rowIdOnly).hasSize(1);
        assertThat(rowIdOnly.get(0).getField(0)).isNotNull();

        // verify selecting all columns from row_tracking works
        List<Row> allColumns = batchSql("SELECT * FROM blob_table$row_tracking");
        assertThat(allColumns).hasSize(1);
    }

    @Test
    public void testWriteBlobWithHttpUrlDescriptor() throws Exception {
        TestHttpWebServer httpServer = new TestHttpWebServer("/blob_data");
        httpServer.start();
        try {
            String blobContent = "hello-http-blob";
            String httpUrl = httpServer.getBaseUrl();

            // Enqueue response for the write phase
            httpServer.enqueueResponse(blobContent, 200);

            // Use sys.path_to_descriptor with HTTP URL
            batchSql(
                    "INSERT INTO blob_table_descriptor VALUES (1, 'http-blob', sys.path_to_descriptor('"
                            + httpUrl
                            + "'))");

            // Read back with blob-as-descriptor=false to get raw data
            batchSql("ALTER TABLE blob_table_descriptor SET ('blob-as-descriptor'='false')");
            List<Row> result = batchSql("SELECT * FROM blob_table_descriptor");
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getField(0)).isEqualTo(1);
            assertThat(result.get(0).getField(1)).isEqualTo("http-blob");
            assertThat((byte[]) result.get(0).getField(2))
                    .isEqualTo(blobContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } finally {
            httpServer.stop();
        }
    }

    @Test
    public void testWriteFetchFailureDescriptorWritesNull() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE fetch_failure_blob_table (id INT, data STRING, picture BYTES)"
                        + " WITH ('row-tracking.enabled'='true',"
                        + " 'data-evolution.enabled'='true',"
                        + " 'blob-field'='picture',"
                        + " 'blob-as-descriptor'='true',"
                        + " 'blob-write-null-on-fetch-failure'='true')");

        batchSql(
                "INSERT INTO fetch_failure_blob_table VALUES"
                        + " (1, 'invalid-uri', sys.path_to_descriptor('https://img.alicdn.com/imgextra/##1304008055350781673'))");

        List<Row> result = batchSql("SELECT * FROM fetch_failure_blob_table");
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getField(0)).isEqualTo(1);
        assertThat(result.get(0).getField(1)).isEqualTo("invalid-uri");
        assertThat(result.get(0).getField(2)).isNull();
    }

    @Test
    public void testWriteHttpBadRequestWritesNullWithFetchFailure() throws Exception {
        TestHttpWebServer httpServer = new TestHttpWebServer("/bad_request_blob");
        httpServer.start();
        try {
            String httpUrl = httpServer.getBaseUrl();
            httpServer.enqueueResponse("", 400);
            httpServer.enqueueResponse("", 400);

            tEnv.executeSql(
                    "CREATE TABLE bad_request_blob_table (id INT, data STRING, picture BYTES)"
                            + " WITH ('row-tracking.enabled'='true',"
                            + " 'data-evolution.enabled'='true',"
                            + " 'blob-field'='picture',"
                            + " 'blob-as-descriptor'='true',"
                            + " 'blob-write-null-on-fetch-failure'='true')");

            batchSql(
                    "INSERT INTO bad_request_blob_table VALUES"
                            + " (1, 'bad-request', sys.path_to_descriptor('"
                            + httpUrl
                            + "'))");

            List<Row> result = batchSql("SELECT * FROM bad_request_blob_table");
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getField(0)).isEqualTo(1);
            assertThat(result.get(0).getField(1)).isEqualTo("bad-request");
            assertThat(result.get(0).getField(2)).isNull();
        } finally {
            httpServer.stop();
        }
    }

    @Test
    public void testWriteHttpRateLimitWritesNullWithFetchFailure() throws Exception {
        TestHttpWebServer httpServer = new TestHttpWebServer("/rate_limit_blob");
        httpServer.start();
        try {
            String httpUrl = httpServer.getBaseUrl();
            httpServer.enqueueResponse("", 420);
            httpServer.enqueueResponse("", 420);

            tEnv.executeSql(
                    "CREATE TABLE rate_limit_blob_table (id INT, data STRING, picture BYTES)"
                            + " WITH ('row-tracking.enabled'='true',"
                            + " 'data-evolution.enabled'='true',"
                            + " 'blob-field'='picture',"
                            + " 'blob-as-descriptor'='true',"
                            + " 'blob-write-null-on-fetch-failure'='true')");

            batchSql(
                    "INSERT INTO rate_limit_blob_table VALUES"
                            + " (1, 'rate-limit', sys.path_to_descriptor('"
                            + httpUrl
                            + "'))");

            List<Row> result = batchSql("SELECT * FROM rate_limit_blob_table");
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getField(0)).isEqualTo(1);
            assertThat(result.get(0).getField(1)).isEqualTo("rate-limit");
            assertThat(result.get(0).getField(2)).isNull();
        } finally {
            httpServer.stop();
        }
    }

    @Test
    public void testWriteInvalidUriWritesNullWithMissingFileAndFetchFailure() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE combined_null_blob_table (id INT, data STRING, picture BYTES)"
                        + " WITH ('row-tracking.enabled'='true',"
                        + " 'data-evolution.enabled'='true',"
                        + " 'blob-field'='picture',"
                        + " 'blob-as-descriptor'='true',"
                        + " 'blob-write-null-on-missing-file'='true',"
                        + " 'blob-write-null-on-fetch-failure'='true')");

        batchSql(
                "INSERT INTO combined_null_blob_table VALUES"
                        + " (1, 'invalid-uri', sys.path_to_descriptor('https://img.alicdn.com/imgextra/##1304008055350781673'))");

        List<Row> result = batchSql("SELECT * FROM combined_null_blob_table");
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getField(0)).isEqualTo(1);
        assertThat(result.get(0).getField(1)).isEqualTo("invalid-uri");
        assertThat(result.get(0).getField(2)).isNull();
    }

    @Test
    public void testWriteHttpBadRequestWritesNullWithMissingFileAndFetchFailure() throws Exception {
        TestHttpWebServer httpServer = new TestHttpWebServer("/combined_bad_request_blob");
        httpServer.start();
        try {
            String httpUrl = httpServer.getBaseUrl();
            httpServer.enqueueResponse("", 400);
            httpServer.enqueueResponse("", 400);

            tEnv.executeSql(
                    "CREATE TABLE combined_bad_request_blob_table (id INT, data STRING, picture BYTES)"
                            + " WITH ('row-tracking.enabled'='true',"
                            + " 'data-evolution.enabled'='true',"
                            + " 'blob-field'='picture',"
                            + " 'blob-as-descriptor'='true',"
                            + " 'blob-write-null-on-missing-file'='true',"
                            + " 'blob-write-null-on-fetch-failure'='true')");

            batchSql(
                    "INSERT INTO combined_bad_request_blob_table VALUES"
                            + " (1, 'bad-request', sys.path_to_descriptor('"
                            + httpUrl
                            + "'))");

            List<Row> result = batchSql("SELECT * FROM combined_bad_request_blob_table");
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getField(0)).isEqualTo(1);
            assertThat(result.get(0).getField(1)).isEqualTo("bad-request");
            assertThat(result.get(0).getField(2)).isNull();
        } finally {
            httpServer.stop();
        }
    }

    @Test
    public void testBlobTypeSchemaEquals() throws Exception {
        // Step 1: Create a Paimon table with blob field via Flink SQL
        tEnv.executeSql(
                "CREATE TABLE blob_schema_test ("
                        + "id INT, "
                        + "name STRING, "
                        + "picture BYTES"
                        + ") WITH ("
                        + "'row-tracking.enabled'='true',"
                        + "'data-evolution.enabled'='true',"
                        + "'blob-field'='picture'"
                        + ")");

        // Step 2: Get the Paimon FileStoreTable and its RowType
        FileStoreTable paimonTable = paimonTable("blob_schema_test");
        RowType paimonRowType = paimonTable.rowType();

        // Step 3: Create a Flink temporary table with the same schema (BYTES column)
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE flink_temp_table ("
                        + "id INT, "
                        + "name STRING, "
                        + "picture BYTES"
                        + ") WITH ("
                        + "'row-tracking.enabled'='true',"
                        + "'data-evolution.enabled'='true',"
                        + "'connector'='blackhole'"
                        + ")");
        org.apache.flink.table.types.logical.RowType flinkRowType =
                (org.apache.flink.table.types.logical.RowType)
                        tEnv.from("flink_temp_table")
                                .getResolvedSchema()
                                .toPhysicalRowDataType()
                                .getLogicalType();

        // Step 4: Convert Paimon RowType to Flink RowType via LogicalTypeConversion
        org.apache.flink.table.types.logical.RowType convertedRowType =
                toLogicalType(paimonRowType);

        // Step 5: Assert that schemaEquals considers them equal
        assertThat(AbstractFlinkTableFactory.schemaEquals(convertedRowType, flinkRowType)).isTrue();
    }

    @Test
    public void testBlobDescriptorFieldStreamingWriteWithShuffle() throws Exception {
        // Verifies that blob-descriptor-field works when shuffle serialization occurs
        // in streaming mode. When source and sink have different parallelism, Flink
        // inserts a REBALANCE that serializes InternalRow to BinaryRow via
        // InternalRowTypeSerializer. writeBlob() must preserve the BlobDescriptor so
        // that BlobDescriptorWriter can serialize it at the sink.

        byte[] blobData = "multimodal-content".getBytes();
        FileIO fileIO = new LocalFileIO();
        String uri = "file://" + warehouse + "/multimodal_blob_stream";
        try (OutputStream outputStream =
                fileIO.newOutputStream(new org.apache.paimon.fs.Path(uri), true)) {
            outputStream.write(blobData);
        }

        sEnv.executeSql(
                "CREATE TABLE multimodal_assets_metadata ("
                        + "id STRING, "
                        + "source_uri STRING, "
                        + "media_content BYTES, "
                        + "dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "'row-tracking.enabled'='true',"
                        + "'data-evolution.enabled'='true',"
                        + "'blob-field'='media_content',"
                        + "'blob-descriptor-field'='media_content'"
                        + ")");

        // Streaming INSERT with sys.path_to_descriptor
        // The /*+ OPTIONS('sink.parallelism' = '2') */ hint forces a shuffle
        // from the source (parallelism 1 for VALUES) to the sink (parallelism 2).
        // After fix, writeBlob() preserves BlobDescriptor through the shuffle.
        sEnv.getConfig().set("table.dml-sync", "true");
        sEnv.executeSql(
                        "INSERT INTO multimodal_assets_metadata "
                                + "/*+ OPTIONS('sink.parallelism' = '2') */ "
                                + "VALUES ('test-id', 'test-uri', "
                                + "sys.path_to_descriptor('"
                                + uri
                                + "'), '2025-07-07')")
                .await();
    }

    @Test
    public void testBlobDescriptorFieldBatchWriteWithShuffle() throws Exception {
        // Same as streaming test but in batch mode.
        // Verifies that blob-descriptor-field works when shuffle serialization
        // occurs in batch mode.

        byte[] blobData = "batch-multimodal-content".getBytes();
        FileIO fileIO = new LocalFileIO();
        String uri = "file://" + warehouse + "/multimodal_blob_batch";
        try (OutputStream outputStream =
                fileIO.newOutputStream(new org.apache.paimon.fs.Path(uri), true)) {
            outputStream.write(blobData);
        }

        tEnv.executeSql(
                "CREATE TABLE batch_multimodal_metadata ("
                        + "id STRING, "
                        + "source_uri STRING, "
                        + "media_content BYTES, "
                        + "dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "'row-tracking.enabled'='true',"
                        + "'data-evolution.enabled'='true',"
                        + "'blob-field'='media_content',"
                        + "'blob-descriptor-field'='media_content'"
                        + ")");

        // Batch INSERT with sys.path_to_descriptor
        // The /*+ OPTIONS('sink.parallelism' = '2') */ hint forces a shuffle,
        // same as streaming mode.
        batchSql(
                "INSERT INTO batch_multimodal_metadata "
                        + "/*+ OPTIONS('sink.parallelism' = '2') */ "
                        + "VALUES ('test-id', 'test-uri', "
                        + "sys.path_to_descriptor('"
                        + uri
                        + "'), '2025-07-07')");
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}
