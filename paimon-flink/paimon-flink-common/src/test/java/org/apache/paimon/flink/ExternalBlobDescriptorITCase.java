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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.UriReaderFactory;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for external blob descriptors. */
public class ExternalBlobDescriptorITCase extends CatalogITCaseBase {

    private static final Random RANDOM = new Random();

    @TempDir private Path warehouse;

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS blob_table_descriptor (id INT, data STRING, picture BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture', 'blob-as-descriptor'='true')",
                "CREATE TABLE external_blob_table_descriptor (id INT, data STRING, picture BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture', 'blob-as-descriptor'='true', 'blob-descriptor.root-dir'='"
                        + "isolated://"
                        + warehouse
                        + "')");
    }

    @Override
    protected Map<String, String> catalogOptions() {
        return Collections.singletonMap("root-dir", path);
    }

    @Override
    protected String getTempDirPath() {
        // make paimon use isolated file io.
        return "isolated://" + super.getTempDirPath();
    }

    @Test
    public void testWriteBlobDescriptorFromExternalStorage() throws Exception {
        byte[] blobData = new byte[1024 * 1024];
        RANDOM.nextBytes(blobData);
        FileIO fileIO = new LocalFileIO();
        String blobPath = warehouse + "/external_blob";
        try (OutputStream outputStream =
                fileIO.newOutputStream(new org.apache.paimon.fs.Path("file://" + blobPath), true)) {
            outputStream.write(blobData);
        }

        String isolatedPath = "isolated://" + blobPath;

        // directly write should raise an error
        Assertions.assertThatThrownBy(
                        () ->
                                batchSql(
                                        "INSERT INTO blob_table_descriptor VALUES (1, 'paimon', sys.path_to_descriptor('"
                                                + isolatedPath
                                                + "'))"))
                .isInstanceOf(Exception.class)
                .hasRootCauseInstanceOf(UnsupportedOperationException.class);

        // however, we can specify io config for input descriptors
        // this mocks using paimon through connector mode (not catalog mode)
        batchSql(
                "INSERT INTO external_blob_table_descriptor VALUES (1, 'paimon', sys.path_to_descriptor('"
                        + isolatedPath
                        + "'))");
        byte[] newDescriptorBytes =
                (byte[])
                        batchSql("SELECT picture FROM external_blob_table_descriptor")
                                .get(0)
                                .getField(0);

        BlobDescriptor newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes);
        assertBlobEquals(blobData, newBlobDescriptor);

        // alternatively, we could also set options through dynamic options
        tEnv.getConfig()
                .getConfiguration()
                .setString(
                        "paimon.*.*.blob_table_descriptor."
                                + CoreOptions.BLOB_DESCRIPTOR_PREFIX
                                + IsolatedDirectoryFileIO.ROOT_DIR,
                        "isolated://" + warehouse);
        batchSql(
                "INSERT INTO blob_table_descriptor VALUES (1, 'paimon', sys.path_to_descriptor('"
                        + isolatedPath
                        + "'))");
        newDescriptorBytes =
                (byte[]) batchSql("SELECT picture FROM blob_table_descriptor").get(0).getField(0);
        newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes);
        assertBlobEquals(blobData, newBlobDescriptor);
    }

    private void assertBlobEquals(byte[] expected, BlobDescriptor readDescriptor) {
        Options options = new Options();
        options.set("root-dir", path);
        CatalogContext catalogContext = CatalogContext.create(options);
        UriReaderFactory uriReaderFactory = new UriReaderFactory(catalogContext);
        Blob blob =
                Blob.fromDescriptor(uriReaderFactory.create(readDescriptor.uri()), readDescriptor);
        assertThat(blob.toData()).isEqualTo(expected);
    }
}
