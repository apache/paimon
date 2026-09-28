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
import org.apache.paimon.oss.FakeRESTAndOSSServer;
import org.apache.paimon.oss.OSSFileIO;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

    private static RESTTokenFileIO restTokenFileIO(FakeRESTAndOSSServer server, String table) {
        Options options = server.catalogOptions();
        options.set("fs.oss.multipart.download.size", "1024");
        options.set("fs.oss.multipart.download.threads", "1");
        return new RESTTokenFileIO(
                CatalogContext.create(
                        options, new Configuration(false), new PluginOSSLoader(), null),
                null,
                Identifier.create("db", table),
                new Path("oss://bucket/" + table));
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
