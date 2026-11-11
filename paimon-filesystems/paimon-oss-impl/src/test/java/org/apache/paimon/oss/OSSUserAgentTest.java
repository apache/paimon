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

package org.apache.paimon.oss;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import com.aliyun.oss.common.utils.VersionInfoUtils;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OSSUserAgent}. */
public class OSSUserAgentTest {

    private static final String SDK = "aliyun-sdk-java/" + OSSBuildVersions.OSS_SDK;

    private static final String EMPTY_LISTING =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    + "<ListBucketResult><Name>bucket</Name><MaxKeys>1</MaxKeys>"
                    + "<IsTruncated>false</IsTruncated><KeyCount>0</KeyCount></ListBucketResult>";

    @Test
    public void testDefault() {
        // The version written in at build time matches the SDK jar actually bundled.
        assertThat(OSSBuildVersions.OSS_SDK).isEqualTo(VersionInfoUtils.getVersion());
        assertThat(OSSBuildVersions.PAIMON).matches("\\d+\\.\\d+\\S*");
        assertThat(OSSUserAgent.prefix(new Options()))
                .isEqualTo("Paimon/" + OSSBuildVersions.PAIMON + "(" + SDK + ")");
    }

    @Test
    public void testModuleAndFeatures() {
        Options options = new Options();
        options.set(OSSUserAgent.MODULE, "MyApp/1.0");
        options.set(OSSUserAgent.FEATURES, " Flink  Paimon ");

        assertThat(OSSUserAgent.prefix(options)).isEqualTo("MyApp/1.0(" + SDK + ";Flink;Paimon)");
    }

    @Test
    public void testAccessTrackingIsAppendedToUserExtended() {
        Options options = new Options();
        options.set(OSSUserAgent.MODULE, "MyApp/1.0");
        options.set(OSSUserAgent.EXTENDED, "vvr");
        options.set(OSSUserAgent.DLF_ACCESS_TRACKING_EXTENDED_INFO, "uid/123 user/alice");

        assertThat(OSSUserAgent.prefix(options))
                .isEqualTo("MyApp/1.0(" + SDK + ") vvr uid/123 user/alice");
    }

    @Test
    public void testAccessTrackingOnly() {
        Options options = new Options();
        options.set(OSSUserAgent.MODULE, "MyApp/1.0");
        options.set(OSSUserAgent.EXTENDED, " ");
        options.set(OSSUserAgent.DLF_ACCESS_TRACKING_EXTENDED_INFO, "uid/123");

        assertThat(OSSUserAgent.prefix(options)).isEqualTo("MyApp/1.0(" + SDK + ") uid/123");
    }

    @Test
    public void testCatalogWideKeys() {
        Options options = new Options();
        options.set(OSSUserAgent.GENERIC_MODULE, "MyApp/1.0");
        options.set(OSSUserAgent.GENERIC_FEATURES, "Flink");
        options.set(OSSUserAgent.GENERIC_EXTENDED, "vvr");
        options.set(OSSUserAgent.DLF_ACCESS_TRACKING_EXTENDED_INFO, "uid/123");

        assertThat(OSSUserAgent.prefix(options))
                .isEqualTo("MyApp/1.0(" + SDK + ";Flink) vvr uid/123");
    }

    @Test
    public void testOssKeysOverrideCatalogWideKeysPerPart() {
        Options options = new Options();
        options.set(OSSUserAgent.GENERIC_MODULE, "MyApp/1.0");
        options.set(OSSUserAgent.GENERIC_FEATURES, "Flink");
        options.set(OSSUserAgent.GENERIC_EXTENDED, "vvr");
        options.set(OSSUserAgent.FEATURES, "Spark");
        options.set(OSSUserAgent.EXTENDED, " ");

        assertThat(OSSUserAgent.prefix(options)).isEqualTo("MyApp/1.0(" + SDK + ";Spark) vvr");
    }

    @Test
    public void testUserPrefixWins() {
        Options options = new Options();
        options.set(OSSUserAgent.PREFIX, "custom");
        options.set(OSSUserAgent.EXTENDED, "vvr");
        OSSFileIO fileIO = new OSSFileIO();
        fileIO.configure(CatalogContext.create(options));

        assertThat(fileIO.hadoopOptions().get(OSSUserAgent.PREFIX)).isEqualTo("custom");
    }

    @Test
    public void testUserAgentOnTheWire() throws IOException {
        List<String> userAgents = new CopyOnWriteArrayList<>();
        HttpServer server =
                HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    userAgents.add(exchange.getRequestHeaders().getFirst("User-Agent"));
                    exchange.getResponseHeaders().add("x-oss-request-id", "fake-request-id");
                    if ("HEAD".equals(exchange.getRequestMethod())) {
                        exchange.sendResponseHeaders(404, -1);
                    } else {
                        byte[] body = EMPTY_LISTING.getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().add("Content-Type", "application/xml");
                        exchange.sendResponseHeaders(200, body.length);
                        exchange.getResponseBody().write(body);
                    }
                    exchange.close();
                });
        server.start();
        try {
            Options options = new Options();
            options.set("fs.oss.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
            options.set("fs.oss.accessKeyId", "ak");
            options.set("fs.oss.accessKeySecret", "sk");
            options.set("fs.oss.attempts.maximum", "1");
            options.set("file-io.allow-cache", "false");
            options.set(OSSUserAgent.GENERIC_FEATURES, "Flink");
            options.set(OSSUserAgent.GENERIC_EXTENDED, "vvr");
            options.set(OSSUserAgent.DLF_ACCESS_TRACKING_EXTENDED_INFO, "uid/123");
            OSSFileIO fileIO = new OSSFileIO();
            fileIO.configure(CatalogContext.create(options));

            assertThat(fileIO.exists(new Path("oss://bucket/dir/file"))).isFalse();
            assertThat(userAgents)
                    .isNotEmpty()
                    .allSatisfy(
                            ua ->
                                    assertThat(ua)
                                            .startsWith(OSSUserAgent.prefix(options) + ", Hadoop/")
                                            .contains("(" + SDK + ";Flink) vvr uid/123"));
        } finally {
            server.stop(0);
        }
    }
}
