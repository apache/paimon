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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.UriReaderFactory;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test write and read table with blob type. */
public class BlobTableITCase extends CatalogITCaseBase {

    private static final Random RANDOM = new Random();
    @TempDir private Path warehouse;

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS blob_table (id INT, data STRING, picture BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture')",
                "CREATE TABLE IF NOT EXISTS blob_table_descriptor (id INT, data STRING, picture BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture', 'blob-as-descriptor'='true')");
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
