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

package org.apache.paimon.jindo;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.apache.paimon.rest.RESTCatalogOptions.DLF_FILE_IO_CACHE_ENABLED;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_FILE_IO_CACHE_POLICY;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_FILE_IO_CACHE_WHITELIST_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for Jindo cache enable configuration. */
public class TestJindoCacheEnable {

    private static final String JINDO_CACHE_RPC_ADDRESS = "fs.jindocache.namespace.rpc.address";

    private CatalogContext createCatalogContext(
            boolean cacheEnabled,
            String cachePolicy,
            boolean setRpcAddress,
            String cacheWhitelist) {
        Options options = new Options();
        options.set(DLF_FILE_IO_CACHE_ENABLED, cacheEnabled);
        if (cachePolicy != null) {
            options.set(DLF_FILE_IO_CACHE_POLICY, cachePolicy);
        }
        if (setRpcAddress) {
            options.set(JINDO_CACHE_RPC_ADDRESS, "test-rpc-address:8080");
        }
        if (cacheWhitelist != null) {
            options.set(DLF_FILE_IO_CACHE_WHITELIST_PATH, cacheWhitelist);
        }
        return CatalogContext.create(options);
    }

    private boolean getCacheFlag(HadoopCompliantFileIO fileIO, String fieldName) {
        try {
            Field field = HadoopCompliantFileIO.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.getBoolean(fileIO);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to access field: " + fieldName, e);
        }
    }

    private void verifyCacheFlags(
            HadoopCompliantFileIO fileIO,
            boolean expectedMeta,
            boolean expectedRead,
            boolean expectedWrite) {
        assertThat(getCacheFlag(fileIO, "metaCacheEnabled")).isEqualTo(expectedMeta);
        assertThat(getCacheFlag(fileIO, "readCacheEnabled")).isEqualTo(expectedRead);
        assertThat(getCacheFlag(fileIO, "writeCacheEnabled")).isEqualTo(expectedWrite);
    }

    @Test
    public void testCacheEnabledWithMetaPolicy() {
        CatalogContext context = createCatalogContext(true, "meta", true, null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, true, false, false);
    }

    @Test
    public void testCacheEnabledWithReadPolicy() {
        CatalogContext context = createCatalogContext(true, "read", true, null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, true, false);
    }

    @Test
    public void testCacheEnabledWithWritePolicy() {
        CatalogContext context = createCatalogContext(true, "write", true, null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, false, true);
    }

    @Test
    public void testCacheEnabledWithCombinedPolicy() {
        CatalogContext context = createCatalogContext(true, "meta,read,write", true, null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, true, true, true);
    }

    @Test
    public void testCacheDisabledWhenEnabledFalse() {
        CatalogContext context = createCatalogContext(false, "meta,read,write", true, null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, false, false);
    }

    @Test
    public void testCacheDisabledWhenPolicyNone() {
        CatalogContext context = createCatalogContext(true, "none", true, null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, false, false);
    }

    @Test
    public void testCacheDisabledWhenRpcAddressMissing() {
        CatalogContext context = createCatalogContext(true, "meta,read,write", false, null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, false, false);
    }

    @Test
    public void testCacheDisabledWhenPolicyNull() {
        CatalogContext context = createCatalogContext(true, null, true, null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, false, false);
    }

    @Test
    public void testCacheWhitelist() {
        // default config
        CatalogContext context = createCatalogContext(true, "meta,read,write", true, null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);
        assertThat(
                        fileIO.shouldCache(
                                new Path("oss://test-bucket/database/table/bucket-0/file.orc")))
                .isTrue();
        assertThat(
                        fileIO.shouldCache(
                                new Path("oss://test-bucket/database/table/manifest/manifest-111")))
                .isTrue();
        assertThat(
                        fileIO.shouldCache(
                                new Path("oss://test-bucket/database/table/snapshot/snapshot-1")))
                .isFalse();

        // set whitelist config as *
        context = createCatalogContext(true, "meta,read,write", true, "*");
        fileIO = new JindoFileIO();
        fileIO.configure(context);
        assertThat(
                        fileIO.shouldCache(
                                new Path("oss://test-bucket/database/table/snapshot/snapshot-1")))
                .isTrue();
        assertThat(fileIO.shouldCache(new Path("oss://test-bucket/database/table/dir/file")))
                .isTrue();
    }
}
