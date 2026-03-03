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
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_TRACKING_REPORT_ENABLED;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_OSS_ENDPOINT;
import static org.apache.paimon.rest.RESTTokenFileIO.DLF_ACCESS_TRACKING_EXTENDED_INFO;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RESTTokenFileIO} mergeTokenWithCatalogOptions method. */
public class RESTTokenFileIOMergeTokenTest {

    @SuppressWarnings("unchecked")
    private Map<String, String> invokeMergeTokenWithCatalogOptions(
            RESTTokenFileIO fileIO, Map<String, String> token) throws Exception {
        Method method =
                RESTTokenFileIO.class.getDeclaredMethod("mergeTokenWithCatalogOptions", Map.class);
        method.setAccessible(true);
        return (Map<String, String>) method.invoke(fileIO, token);
    }

    private RESTTokenFileIO createFileIO(Options options) {
        CatalogContext context = CatalogContext.create(options);
        return new RESTTokenFileIO(
                context, null, Identifier.create("db", "table"), new Path("/tmp/test"));
    }

    /** When tracking is explicitly disabled, extended info should be removed from token. */
    @Test
    public void testExtendedInfoRemovedWhenTrackingDisabled() throws Exception {
        Options options = new Options();
        options.set(DLF_ACCESS_TRACKING_REPORT_ENABLED, false);
        RESTTokenFileIO fileIO = createFileIO(options);

        Map<String, String> token = new HashMap<>();
        token.put("fs.oss.accessKeyId", "testAk");
        token.put(DLF_ACCESS_TRACKING_EXTENDED_INFO.key(), "some-tracking-info");

        Map<String, String> result = invokeMergeTokenWithCatalogOptions(fileIO, token);

        assertThat(result).doesNotContainKey(DLF_ACCESS_TRACKING_EXTENDED_INFO.key());
        assertThat(result).containsKey("fs.oss.accessKeyId");
    }

    /** When tracking is explicitly enabled, extended info should be kept in token. */
    @Test
    public void testExtendedInfoKeptWhenTrackingEnabled() throws Exception {
        Options options = new Options();
        options.set(DLF_ACCESS_TRACKING_REPORT_ENABLED, true);
        RESTTokenFileIO fileIO = createFileIO(options);

        Map<String, String> token = new HashMap<>();
        token.put("fs.oss.accessKeyId", "testAk");
        token.put(DLF_ACCESS_TRACKING_EXTENDED_INFO.key(), "some-tracking-info");

        Map<String, String> result = invokeMergeTokenWithCatalogOptions(fileIO, token);

        assertThat(result)
                .containsEntry(DLF_ACCESS_TRACKING_EXTENDED_INFO.key(), "some-tracking-info");
    }

    /**
     * When tracking report option is not explicitly set, the default value is true, so extended
     * info should be kept in token.
     */
    @Test
    public void testExtendedInfoKeptWhenTrackingDefault() throws Exception {
        Options options = new Options();
        RESTTokenFileIO fileIO = createFileIO(options);

        Map<String, String> token = new HashMap<>();
        token.put("fs.oss.accessKeyId", "testAk");
        token.put(DLF_ACCESS_TRACKING_EXTENDED_INFO.key(), "default-tracking-info");

        Map<String, String> result = invokeMergeTokenWithCatalogOptions(fileIO, token);

        assertThat(result)
                .containsEntry(DLF_ACCESS_TRACKING_EXTENDED_INFO.key(), "default-tracking-info");
    }

    /** DLF OSS endpoint should override the standard OSS endpoint in token. */
    @Test
    public void testDlfOssEndpointOverride() throws Exception {
        Options options = new Options();
        options.set(DLF_OSS_ENDPOINT, "oss-cn-hangzhou-internal.aliyuncs.com");
        RESTTokenFileIO fileIO = createFileIO(options);

        Map<String, String> token = new HashMap<>();
        token.put("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");

        Map<String, String> result = invokeMergeTokenWithCatalogOptions(fileIO, token);

        assertThat(result)
                .containsEntry("fs.oss.endpoint", "oss-cn-hangzhou-internal.aliyuncs.com");
    }

    /**
     * When tracking is disabled but token does not contain extended info, remove should be a safe
     * no-op.
     */
    @Test
    public void testRemoveExtendedInfoWhenNotPresentInToken() throws Exception {
        Options options = new Options();
        options.set(DLF_ACCESS_TRACKING_REPORT_ENABLED, false);
        RESTTokenFileIO fileIO = createFileIO(options);

        Map<String, String> token = new HashMap<>();
        token.put("fs.oss.accessKeyId", "testAk");

        Map<String, String> result = invokeMergeTokenWithCatalogOptions(fileIO, token);

        assertThat(result).doesNotContainKey(DLF_ACCESS_TRACKING_EXTENDED_INFO.key());
        assertThat(result).containsKey("fs.oss.accessKeyId");
    }
}
