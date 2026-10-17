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
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PluginFileIO;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.oss.FakeRESTAndOSSServer;
import org.apache.paimon.oss.OSSFileIO;
import org.apache.paimon.oss.StubStorageOSSFileIO;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link RESTTokenFileIO} over a real {@link OSSFileIO} against a local catalog and OSS. */
public class RESTTokenFileIOOnOSSTest {

    /** An open stream keeps refreshing even after its delegate FileIO left the cache. */
    @Test
    public void testOpenStreamPicksUpRefreshedTokenAfterCacheEviction() throws Exception {
        try (FakeRESTAndOSSServer server = new FakeRESTAndOSSServer()) {
            long now = System.currentTimeMillis();
            server.addToken("ak-1", now + Duration.ofHours(2).toMillis());
            server.addToken("ak-2", now + Duration.ofHours(4).toMillis());
            RESTTokenFileIO fileIO = restTokenFileIO(server, "table");

            try (SeekableInputStream in =
                    fileIO.newInputStream(new Path("oss://bucket/table/data"))) {
                in.read(new byte[1024]);

                RESTTokenFileIO.invalidateFileIOCache();
                System.gc();
                // 30 minutes left is inside the refresh window
                RESTTokenRefresher.clockOffsetMillis = Duration.ofMinutes(90).toMillis();
                in.seek(3000);
                in.read(new byte[1024]);
            } finally {
                RESTTokenRefresher.clockOffsetMillis = 0;
            }

            List<String> authorizations = server.ossGetAuthorizations();
            assertThat(authorizations.get(0)).contains("ak-1");
            assertThat(authorizations.get(authorizations.size() - 1)).contains("ak-2");
        }
    }

    /** A stream reloads the token of its own table, even when another table got the same one. */
    @Test
    public void testStreamReloadsTheTokenOfItsOwnTable() throws Exception {
        try (FakeRESTAndOSSServer server = new FakeRESTAndOSSServer()) {
            server.addToken("ak-1", System.currentTimeMillis() + Duration.ofHours(2).toMillis());
            RESTTokenFileIO tableA = restTokenFileIO(server, "table_a");
            RESTTokenFileIO tableB = restTokenFileIO(server, "table_b");

            tableA.exists(new Path("oss://bucket/table_a/data"));
            try (SeekableInputStream in =
                    tableB.newInputStream(new Path("oss://bucket/table_b/data"))) {
                in.read(new byte[1024]);
                RESTTokenRefresher.clockOffsetMillis = Duration.ofMinutes(90).toMillis();
                in.seek(3000);
                in.read(new byte[1024]);
            } finally {
                RESTTokenRefresher.clockOffsetMillis = 0;
            }

            List<String> paths = server.tokenRequestPaths();
            assertThat(paths.get(paths.size() - 1)).endsWith("/tables/table_b/token");
        }
    }

    /** A presigned URL is signed with a token that outlives it, not one rotated in meanwhile. */
    @Test
    public void testPresignsWithATokenThatOutlivesTheUrl() throws Exception {
        try (FakeRESTAndOSSServer server = new FakeRESTAndOSSServer()) {
            long now = System.currentTimeMillis();
            server.addToken("ak-1", now + Duration.ofMinutes(61).toMillis());
            // rotated in while copying, with less time left than the URL needs
            server.addToken("ak-2", now + Duration.ofMinutes(31 + 5).toMillis());
            server.addToken("ak-3", now + Duration.ofHours(4).toMillis());

            String url = presignWhileCopyingTakes31Minutes(server);

            assertThat(url).contains("OSSAccessKeyId=ak-3").doesNotContain("ak-2");
            assertThat(server.tokenRequests()).isEqualTo(3);
        }
    }

    /** Presigning fails when the catalog has no token that outlives the URL. */
    @Test
    public void testRejectsPresigningWhenNoTokenOutlivesTheUrl() throws Exception {
        try (FakeRESTAndOSSServer server = new FakeRESTAndOSSServer()) {
            long now = System.currentTimeMillis();
            server.addToken("ak-1", now + Duration.ofMinutes(61).toMillis());
            server.addToken("ak-2", now + Duration.ofMinutes(31 + 5).toMillis());

            assertThatThrownBy(() -> presignWhileCopyingTakes31Minutes(server))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("exceeds the remaining REST credential lifetime");
        }
    }

    /** Copying puts ak-1, which has 61 minutes, past half of its lifetime. */
    private static String presignWhileCopyingTakes31Minutes(FakeRESTAndOSSServer server)
            throws IOException {
        RESTTokenFileIO fileIO =
                restTokenFileIO(
                        server,
                        "table",
                        () ->
                                new StubStorageOSSFileIO(
                                        () ->
                                                RESTTokenRefresher.clockOffsetMillis =
                                                        Duration.ofMinutes(31).toMillis()));
        try {
            return fileIO.createBlobPresignedUrl(
                    new Path("oss://bucket/table"),
                    new BlobDescriptor("oss://bucket/table/bucket-0/source.blob", 10, 20),
                    Duration.ofMinutes(10));
        } finally {
            RESTTokenRefresher.clockOffsetMillis = 0;
        }
    }

    private static RESTTokenFileIO restTokenFileIO(FakeRESTAndOSSServer server, String table) {
        return restTokenFileIO(server, table, OSSFileIO::new);
    }

    private static RESTTokenFileIO restTokenFileIO(
            FakeRESTAndOSSServer server, String table, Supplier<OSSFileIO> ossFileIO) {
        Options options = server.catalogOptions();
        options.set("fs.oss.multipart.download.size", "1024");
        options.set("fs.oss.multipart.download.threads", "1");
        return new RESTTokenFileIO(
                CatalogContext.create(
                        options, new Configuration(false), new PluginOSSLoader(ossFileIO), null),
                null,
                Identifier.create("db", table),
                new Path("oss://bucket/" + table));
    }

    /** Wraps {@link OSSFileIO} the way the OSS plugin does, including its no-op close. */
    private static class PluginOSSLoader implements FileIOLoader {

        private final Supplier<OSSFileIO> ossFileIO;

        private PluginOSSLoader(Supplier<OSSFileIO> ossFileIO) {
            this.ossFileIO = ossFileIO;
        }

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
                    FileIO fileIO = ossFileIO.get();
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
