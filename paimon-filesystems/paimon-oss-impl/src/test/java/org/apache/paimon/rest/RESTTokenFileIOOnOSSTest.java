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

package org.apache.paimon.rest;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PluginFileIO;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.oss.OSSFileIO;
import org.apache.paimon.rest.responses.GetTableTokenResponse;

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests {@link RESTTokenFileIO} over a real {@link OSSFileIO} against a local OSS endpoint. */
public class RESTTokenFileIOOnOSSTest {

    private static final byte[] CONTENT = new byte[4096];

    private final List<String> getAuthorizations = Collections.synchronizedList(new ArrayList<>());
    private HttpServer server;

    @BeforeEach
    public void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    exchange.getResponseHeaders()
                            .add("Last-Modified", "Thu, 24 Sep 2026 00:00:00 GMT");
                    if ("HEAD".equals(exchange.getRequestMethod())) {
                        exchange.getResponseHeaders()
                                .add("Content-Length", String.valueOf(CONTENT.length));
                        exchange.sendResponseHeaders(200, -1);
                    } else {
                        getAuthorizations.add(
                                exchange.getRequestHeaders().getFirst("Authorization"));
                        String[] range =
                                exchange.getRequestHeaders()
                                        .getFirst("Range")
                                        .substring("bytes=".length())
                                        .split("-");
                        int start = Integer.parseInt(range[0]);
                        int end = Math.min(Integer.parseInt(range[1]), CONTENT.length - 1);
                        exchange.getResponseHeaders()
                                .add(
                                        "Content-Range",
                                        "bytes " + start + "-" + end + "/" + CONTENT.length);
                        exchange.sendResponseHeaders(206, end - start + 1);
                        try (OutputStream out = exchange.getResponseBody()) {
                            out.write(CONTENT, start, end - start + 1);
                        }
                    }
                    exchange.close();
                });
        server.start();
    }

    @AfterEach
    public void stopServer() {
        server.stop(0);
    }

    /** An open stream keeps refreshing even after its delegate FileIO left the cache. */
    @Test
    public void testOpenStreamPicksUpRefreshedTokenAfterCacheEviction() throws Exception {
        Identifier identifier = Identifier.create("db", "table");
        AtomicLong now = new AtomicLong(1700000000000L);
        RESTApi api = mock(RESTApi.class);
        when(api.loadTableToken(identifier))
                .thenReturn(
                        new GetTableTokenResponse(
                                token("ak-1"), now.get() + Duration.ofHours(2).toMillis()),
                        new GetTableTokenResponse(
                                token("ak-2"), now.get() + Duration.ofHours(4).toMillis()));
        Options options = new Options();
        options.set("fs.oss.multipart.download.size", "1024");
        options.set("fs.oss.multipart.download.threads", "1");
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(
                                options, new Configuration(false), new PluginOSSLoader(), null),
                        api,
                        identifier,
                        new Path("oss://bucket/table")) {
                    @Override
                    long currentTimeMillis() {
                        return now.get();
                    }
                };

        try (SeekableInputStream in = fileIO.newInputStream(new Path("oss://bucket/table/f"))) {
            in.read(new byte[1024]);

            RESTTokenFileIO.invalidateFileIOCache();
            System.gc();
            // 30 minutes left is inside the refresh window
            now.addAndGet(Duration.ofMinutes(90).toMillis());
            in.seek(3000);
            in.read(new byte[1024]);
        }

        assertThat(getAuthorizations.get(0)).contains("ak-1");
        assertThat(getAuthorizations.get(getAuthorizations.size() - 1)).contains("ak-2");
        verify(api, times(2)).loadTableToken(identifier);
    }

    private Map<String, String> token(String accessKeyId) {
        Map<String, String> token = new HashMap<>();
        token.put("fs.oss.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
        token.put("fs.oss.accessKeyId", accessKeyId);
        token.put("fs.oss.accessKeySecret", "secret-" + accessKeyId);
        token.put("fs.oss.securityToken", "token-" + accessKeyId);
        return token;
    }

    /** Wraps {@link OSSFileIO} the way the OSS plugin does, including its no-op close. */
    private static class PluginOSSLoader implements FileIOLoader {

        @Override
        public String getScheme() {
            return "oss";
        }

        @Override
        public FileIO load(Path path) {
            return new PluginFileIO() {
                @Override
                public boolean isObjectStore() {
                    return true;
                }

                @Override
                protected FileIO createFileIO(Path path) {
                    FileIO fileIO = new OSSFileIO();
                    fileIO.configure(CatalogContext.create(options));
                    return fileIO;
                }

                @Override
                protected ClassLoader pluginClassLoader() {
                    return OSSFileIO.class.getClassLoader();
                }
            };
        }
    }
}
