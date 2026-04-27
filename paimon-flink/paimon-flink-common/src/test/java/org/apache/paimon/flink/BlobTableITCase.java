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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.TestHttpWebServer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.UriReader;
import org.apache.paimon.utils.UriReaderFactory;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.assertj.core.api.Assertions.assertThat;

/** Test write and read table with blob type. */
public class BlobTableITCase extends CatalogITCaseBase {

    private static final Random RANDOM = new Random();
    @TempDir private Path warehouse;

    @Override
    protected List<String> ddl() {
        String externalStoragePath = warehouse.resolve("external-storage-blob-path").toString();
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS blob_table (id INT, data STRING, picture BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture')",
                "CREATE TABLE IF NOT EXISTS blob_table_descriptor (id INT, data STRING, picture BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture', 'blob-as-descriptor'='true')",
                "CREATE TABLE IF NOT EXISTS multiple_blob_table (id INT, data STRING, pic1 BYTES, pic2 BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='pic1,pic2')",
                String.format(
                        "CREATE TABLE IF NOT EXISTS copy_blob_table (id INT, data STRING, picture BYTES)"
                                + " WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true',"
                                + " 'blob-field'='picture', 'blob-descriptor-field'='picture',"
                                + " 'blob-external-storage-field'='picture', 'blob-external-storage-path'='%s')",
                        externalStoragePath),
                String.format(
                        "CREATE TABLE IF NOT EXISTS three_type_blob_table"
                                + " (id INT, data STRING, raw_pic BYTES, desc_pic BYTES, copy_pic BYTES)"
                                + " WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true',"
                                + " 'blob-field'='raw_pic,desc_pic,copy_pic', 'blob-descriptor-field'='desc_pic,copy_pic',"
                                + " 'blob-external-storage-field'='copy_pic', 'blob-external-storage-path'='%s')",
                        externalStoragePath));
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

        int pictureFieldId =
                paimonTable("upstream_blob_view").rowType().getFields().stream()
                        .filter(field -> field.name().equals("picture"))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("picture field not found"))
                        .id();
        String fullTableName = tEnv.getCurrentDatabase() + ".upstream_blob_view";

        tEnv.executeSql(
                "CREATE TABLE downstream_blob_view (id INT, label STRING, image_ref BYTES)"
                        + " WITH ('row-tracking.enabled'='true',"
                        + " 'data-evolution.enabled'='true',"
                        + " 'blob-view-field'='image_ref')");

        batchSql(
                String.format(
                        "INSERT INTO downstream_blob_view"
                                + " SELECT id, name, sys.blob_view('%s', %d, _ROW_ID)"
                                + " FROM `upstream_blob_view$row_tracking`",
                        fullTableName, pictureFieldId));

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
    public void testExternalStorageBlob() throws Exception {
        // Write raw data; descriptor mode with external storage should write to the external path.
        batchSql("INSERT INTO copy_blob_table VALUES (1, 'copy-test', X'48656C6C6F')");

        // Read back — the blob should be readable
        List<Row> result = batchSql("SELECT * FROM copy_blob_table");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, "copy-test", new byte[] {72, 101, 108, 108, 111}));

        // Verify blob files exist in the external storage path
        Path externalStorageDir = warehouse.resolve("external-storage-blob-path");
        assertThat(Files.exists(externalStorageDir)).isTrue();
        try (Stream<Path> stream = Files.list(externalStorageDir)) {
            long externalStorageFiles =
                    stream.filter(p -> p.getFileName().toString().endsWith(".blob")).count();
            assertThat(externalStorageFiles).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    public void testThreeTypeBlobCoexistence() throws Exception {
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

        // raw_pic = raw bytes, desc_pic = descriptor, copy_pic = descriptor with external storage
        batchSql(
                String.format(
                        "INSERT INTO three_type_blob_table VALUES"
                                + " (1, 'three-types', X'48656C6C6F', X'%s', X'5945')",
                        bytesToHex(descriptor.serialize())));

        // Read back and verify all three blob types
        List<Row> result =
                batchSql("SELECT id, data, raw_pic, copy_pic FROM three_type_blob_table");
        assertThat(result.size()).isEqualTo(1);
        Row row = result.get(0);
        assertThat(row.getField(0)).isEqualTo(1);
        assertThat(row.getField(1)).isEqualTo("three-types");
        assertThat((byte[]) row.getField(2)).isEqualTo(new byte[] {72, 101, 108, 108, 111});
        assertThat((byte[]) row.getField(3)).isEqualTo(new byte[] {89, 69});

        // Verify descriptor files backed by external storage exist in the configured path
        Path externalStorageDir = warehouse.resolve("external-storage-blob-path");
        assertThat(Files.exists(externalStorageDir)).isTrue();
        try (Stream<Path> stream = Files.list(externalStorageDir)) {
            long externalStorageFiles =
                    stream.filter(p -> p.getFileName().toString().endsWith(".blob")).count();
            assertThat(externalStorageFiles).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    public void testExternalStorageBlobMultipleWrites() throws Exception {
        // Write two batches; each should create separate descriptor files in external storage.
        batchSql("INSERT INTO copy_blob_table VALUES (1, 'row1', X'48656C6C6F')");
        batchSql("INSERT INTO copy_blob_table VALUES (2, 'row2', X'5945')");

        // Read back
        List<Row> result = batchSql("SELECT * FROM copy_blob_table ORDER BY id");
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0)).isEqualTo(Row.of(1, "row1", new byte[] {72, 101, 108, 108, 111}));
        assertThat(result.get(1)).isEqualTo(Row.of(2, "row2", new byte[] {89, 69}));

        // Verify multiple blob files exist in the external storage path
        Path externalStorageDir = warehouse.resolve("external-storage-blob-path");
        try (Stream<Path> stream = Files.list(externalStorageDir)) {
            long externalStorageFiles =
                    stream.filter(p -> p.getFileName().toString().endsWith(".blob")).count();
            assertThat(externalStorageFiles).isGreaterThanOrEqualTo(2);
        }
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
