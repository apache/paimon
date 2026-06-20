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
import org.apache.paimon.options.Options;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkRowWrapper}. */
public class FlinkRowWrapperTest {

    @TempDir java.nio.file.Path tempPath;

    private HttpServer httpServer;
    private int httpPort;

    @BeforeEach
    public void setUpHttpServer() throws Exception {
        httpServer = HttpServer.create(new InetSocketAddress(0), 0);
        httpPort = httpServer.getAddress().getPort();
        httpServer.start();
    }

    @AfterEach
    public void tearDownHttpServer() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    @Test
    public void testMissingBlobDescriptorIsNullWhenCheckingEnabled() {
        java.nio.file.Path missing = tempPath.resolve("missing.blob");
        GenericRowData row = descriptorRow(missing, 1);

        FlinkRowWrapper wrapper = wrapper(row, true);

        assertThat(wrapper.isNullAt(0)).isTrue();
    }

    @Test
    public void testExistingBlobDescriptorIsReadableWhenCheckingEnabled() throws Exception {
        byte[] bytes = new byte[] {1, 2, 3};
        java.nio.file.Path blobFile = tempPath.resolve("existing.blob");
        Files.write(blobFile, bytes);
        GenericRowData row = descriptorRow(blobFile, bytes.length);

        FlinkRowWrapper wrapper = wrapper(row, true);

        assertThat(wrapper.isNullAt(0)).isFalse();
        assertThat(wrapper.getBlob(0).toData()).isEqualTo(bytes);
    }

    @Test
    public void testMissingHttpBlobDescriptorWithNonBlobColumnBefore() throws Exception {
        httpServer.createContext(
                "/missing.jpg",
                exchange -> {
                    sendResponse(exchange, 404, new byte[0]);
                });
        GenericRowData row =
                GenericRowData.of(
                        1,
                        new BlobDescriptor("http://127.0.0.1:" + httpPort + "/missing.jpg", 0, 1)
                                .serialize());

        FlinkRowWrapper wrapper = wrapper(row, true, Collections.singleton(1));

        assertThat(wrapper.isNullAt(0)).isFalse();
        assertThat(wrapper.getInt(0)).isEqualTo(1);
        assertThat(wrapper.isNullAt(1)).isTrue();
    }

    @Test
    public void testMissingHttpBlobDescriptorIsNullWhenCheckingEnabled() throws Exception {
        httpServer.createContext(
                "/missing.jpg",
                exchange -> {
                    sendResponse(exchange, 404, new byte[0]);
                });
        GenericRowData row = descriptorRow("http://127.0.0.1:" + httpPort + "/missing.jpg", 1);

        FlinkRowWrapper wrapper = wrapper(row, true);

        assertThat(wrapper.isNullAt(0)).isTrue();
    }

    @Test
    public void testExistingHttpBlobDescriptorIsReadableWhenCheckingEnabled() throws Exception {
        byte[] bytes = new byte[] {1, 2, 3};
        httpServer.createContext(
                "/ok.jpg",
                exchange -> {
                    sendResponse(exchange, 200, bytes);
                });
        GenericRowData row =
                descriptorRow("http://127.0.0.1:" + httpPort + "/ok.jpg", bytes.length);

        FlinkRowWrapper wrapper = wrapper(row, true);

        assertThat(wrapper.isNullAt(0)).isFalse();
    }

    @Test
    public void testMissingBlobDescriptorUsesDefaultBehaviorWithoutChecking() {
        java.nio.file.Path missing = tempPath.resolve("missing.blob");
        GenericRowData row = descriptorRow(missing, 1);

        FlinkRowWrapper wrapper = wrapper(row, false);
        Blob blob = wrapper.getBlob(0);

        assertThat(wrapper.isNullAt(0)).isFalse();
        assertThat(blob).isNotNull();
    }

    private GenericRowData descriptorRow(java.nio.file.Path path, long length) {
        return descriptorRow(path.toUri().toString(), length);
    }

    private GenericRowData descriptorRow(String uri, long length) {
        return GenericRowData.of(new BlobDescriptor(uri, 0, length).serialize());
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, byte[] body)
            throws IOException {
        boolean headRequest = "HEAD".equals(exchange.getRequestMethod());
        long responseLength = headRequest ? -1 : body.length;
        exchange.sendResponseHeaders(statusCode, responseLength);
        if (!headRequest && body.length > 0) {
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(body);
            }
        } else {
            exchange.close();
        }
    }

    private FlinkRowWrapper wrapper(GenericRowData row, boolean checkBlobDescriptorExists) {
        return wrapper(row, checkBlobDescriptorExists, Collections.singleton(0));
    }

    private FlinkRowWrapper wrapper(
            GenericRowData row, boolean checkBlobDescriptorExists, Set<Integer> blobFields) {
        return new FlinkRowWrapper(
                row, CatalogContext.create(new Options()), checkBlobDescriptorExists, blobFields);
    }
}
