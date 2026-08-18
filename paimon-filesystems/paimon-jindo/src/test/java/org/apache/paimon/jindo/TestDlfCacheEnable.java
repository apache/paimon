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
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ENDPOINT;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_IGNORE_VPC_VALIDATE;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ORIGIN_ENDPOINT;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_POLICY;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_VPC_VALIDATED;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_WHITELIST;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_WHITELIST_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for DLF cache enable configuration. */
public class TestDlfCacheEnable {

    private static final String TABLE_ROOT = "oss://test-bucket/database/table";

    private static final Path META_PATH = new Path(TABLE_ROOT + "/snapshot/snapshot-1");
    private static final Path MANIFEST_PATH =
            new Path(TABLE_ROOT + "/manifest/manifest-a1b2c3d4-0");
    private static final Path GLOBAL_INDEX_PATH =
            new Path(TABLE_ROOT + "/index/btree-global-index-a1b2c3d4.index");
    private static final Path BUCKET_INDEX_PATH = new Path(TABLE_ROOT + "/index/index-a1b2c3d4-0");
    private static final Path DATA_PATH = new Path(TABLE_ROOT + "/bucket-0/data-a1b2c3d4-0.orc");
    private static final Path OTHER_DATA_PATH =
            new Path(TABLE_ROOT + "/bucket-1/data-a1b2c3d4-0.orc");
    private static final Path FILE_INDEX_PATH =
            new Path(TABLE_ROOT + "/bucket-0/data-a1b2c3d4-0.orc.index");

    private CatalogContext createCatalogContext(String cachePolicy, String cacheWhitelist) {
        return createCatalogContext(cachePolicy, cacheWhitelist, null);
    }

    private CatalogContext createCatalogContext(
            String cachePolicy, String cacheWhitelist, String cacheWhitelistPath) {
        Options options = new Options();
        if (cachePolicy != null) {
            options.set(IO_CACHE_POLICY, cachePolicy);
        }
        options.set("fs.oss.endpoint", "test-oss-endpoint");
        options.set(IO_CACHE_ENDPOINT, "test-dlf-cache-endpoint");
        options.set(IO_CACHE_ORIGIN_ENDPOINT, "test-oss-endpoint");
        options.set(IO_CACHE_VPC_VALIDATED, true);
        if (cacheWhitelist != null) {
            options.set(IO_CACHE_WHITELIST, cacheWhitelist);
        }
        if (cacheWhitelistPath != null) {
            options.set(IO_CACHE_WHITELIST_PATH, cacheWhitelistPath);
        }
        return CatalogContext.create(options);
    }

    private CatalogContext createDlfCacheContext(
            String cachePolicy, Boolean vpcValidated, boolean ignoreVpcValidate) {
        Options options = new Options();
        if (cachePolicy != null) {
            options.set(IO_CACHE_POLICY, cachePolicy);
        }
        options.set("fs.oss.endpoint", "test-oss-endpoint");
        options.set(IO_CACHE_ENDPOINT, "test-dlf-cache-endpoint");
        options.set(IO_CACHE_ORIGIN_ENDPOINT, "test-oss-endpoint");
        if (vpcValidated != null) {
            options.set(IO_CACHE_VPC_VALIDATED, vpcValidated);
        }
        options.set(IO_CACHE_IGNORE_VPC_VALIDATE, ignoreVpcValidate);
        options.set(IO_CACHE_WHITELIST, "data");
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
        CatalogContext context = createCatalogContext("meta", null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, true, false, false);
    }

    @Test
    public void testCacheEnabledWithReadPolicy() {
        CatalogContext context = createCatalogContext("read", null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, true, false);
    }

    @Test
    public void testCacheEnabledWithWritePolicy() {
        CatalogContext context = createCatalogContext("write", null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, false, true);
    }

    @Test
    public void testCacheEnabledWithCombinedPolicy() {
        CatalogContext context = createCatalogContext("meta,read,write", null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, true, true, true);
    }

    @Test
    public void testCacheDisabledWhenPolicyNone() {
        CatalogContext context = createCatalogContext("none", null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, false, false);
    }

    @Test
    public void testDlfCacheDisabledWhenVpcNotValidated() {
        CatalogContext context = createDlfCacheContext("meta,read,write", false, false);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, false, false);
        Path path = new Path("oss://test-bucket/database/table/bucket-0/file.orc");
        assertThat(fileIO.hadoopOptions(path, "read").get("fs.oss.endpoint"))
                .isEqualTo("test-oss-endpoint");
    }

    @Test
    public void testDlfCacheEnabledWhenIgnoringVpcValidation() {
        CatalogContext context = createDlfCacheContext("read", false, true);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, true, false);
        Path path = new Path("oss://test-bucket/database/table/bucket-0/file.orc");
        assertThat(fileIO.hadoopOptions(path, "read").get("fs.oss.endpoint"))
                .isEqualTo("test-dlf-cache-endpoint");
        assertThat(fileIO.hadoopOptions(path, "meta").get("fs.oss.endpoint"))
                .isEqualTo("test-oss-endpoint");
    }

    @Test
    public void testDlfCacheEnabledWhenVpcValidated() {
        CatalogContext context = createDlfCacheContext("read", true, false);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, true, false);
        Path path = new Path("oss://test-bucket/database/table/bucket-0/file.orc");
        assertThat(fileIO.hadoopOptions(path, "read").get("fs.oss.endpoint"))
                .isEqualTo("test-dlf-cache-endpoint");
        assertThat(fileIO.hadoopOptions(path, "meta").get("fs.oss.endpoint"))
                .isEqualTo("test-oss-endpoint");
    }

    @Test
    public void testDlfCacheDefaultsVpcValidationToFalse() {
        CatalogContext context = createDlfCacheContext("meta,read,write", null, false);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, false, false);
    }

    @Test
    public void testIgnoreVpcValidationDoesNotOverrideDisabledPolicy() {
        CatalogContext context = createDlfCacheContext("none", false, true);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, false, false);
    }

    @Test
    public void testCacheDisabledWhenPolicyNull() {
        CatalogContext context = createCatalogContext(null, null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        verifyCacheFlags(fileIO, false, false, false);
    }

    @Test
    public void testDefaultCacheWhitelist() {
        CatalogContext context = createCatalogContext("meta,read,write", null);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        assertThat(fileIO.shouldCache(META_PATH)).isTrue();
        assertThat(fileIO.shouldCache(GLOBAL_INDEX_PATH)).isTrue();
        assertThat(fileIO.shouldCache(BUCKET_INDEX_PATH)).isTrue();
        assertThat(fileIO.shouldCache(DATA_PATH)).isTrue();
        assertThat(fileIO.shouldCache(FILE_INDEX_PATH)).isTrue();
    }

    @Test
    public void testAllSupportedCacheWhitelistFileTypes() {
        CatalogContext context =
                createCatalogContext(
                        "meta,read,write", "meta,global-index,bucket-index,data,file-index");
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        assertThat(fileIO.shouldCache(META_PATH)).isTrue();
        assertThat(fileIO.shouldCache(GLOBAL_INDEX_PATH)).isTrue();
        assertThat(fileIO.shouldCache(BUCKET_INDEX_PATH)).isTrue();
        assertThat(fileIO.shouldCache(DATA_PATH)).isTrue();
        assertThat(fileIO.shouldCache(FILE_INDEX_PATH)).isTrue();
    }

    @Test
    public void testCustomCacheWhitelist() {
        CatalogContext context = createCatalogContext("meta,read,write", "data,file-index");
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        assertThat(fileIO.shouldCache(META_PATH)).isFalse();
        assertThat(fileIO.shouldCache(GLOBAL_INDEX_PATH)).isFalse();
        assertThat(fileIO.shouldCache(BUCKET_INDEX_PATH)).isFalse();
        assertThat(fileIO.shouldCache(DATA_PATH)).isTrue();
        assertThat(fileIO.shouldCache(FILE_INDEX_PATH)).isTrue();
    }

    @Test
    public void testEmptyAndInvalidCacheWhitelistFailClosed() {
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(createCatalogContext("meta,read,write", ""));

        assertThat(fileIO.shouldCache(META_PATH)).isFalse();
        assertThat(fileIO.shouldCache(DATA_PATH)).isFalse();

        fileIO.configure(createCatalogContext("meta,read,write", "manifest,unknown"));

        assertThat(fileIO.shouldCache(META_PATH)).isFalse();
        assertThat(fileIO.shouldCache(DATA_PATH)).isFalse();
    }

    @Test
    public void testCacheWhitelistPath() {
        CatalogContext context = createCatalogContext("meta,read,write", "*", "manifest, bucket-0");
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        assertThat(fileIO.shouldCache(MANIFEST_PATH)).isTrue();
        assertThat(fileIO.shouldCache(META_PATH)).isFalse();
        assertThat(fileIO.shouldCache(DATA_PATH)).isTrue();
        assertThat(fileIO.shouldCache(OTHER_DATA_PATH)).isFalse();
    }

    @Test
    public void testCacheWhitelistAndPathMustBothMatch() {
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(createCatalogContext("meta,read,write", "meta", "manifest"));

        assertThat(fileIO.shouldCache(MANIFEST_PATH)).isTrue();
        assertThat(fileIO.shouldCache(META_PATH)).isFalse();
        assertThat(fileIO.shouldCache(DATA_PATH)).isFalse();
    }

    @Test
    public void testEmptyCacheWhitelistPathFailsClosed() {
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(createCatalogContext("meta,read,write", "*", ""));

        assertThat(fileIO.shouldCache(META_PATH)).isFalse();
        assertThat(fileIO.shouldCache(DATA_PATH)).isFalse();
    }

    @Test
    public void testMutableMetaFilesAreNotCached() {
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(createCatalogContext("meta,read,write", "*"));

        assertThat(fileIO.shouldCache(META_PATH)).isTrue();
        assertThat(fileIO.shouldCache(new Path(TABLE_ROOT + "/snapshot/EARLIEST"))).isFalse();
        assertThat(fileIO.shouldCache(new Path(TABLE_ROOT + "/snapshot/LATEST"))).isFalse();
    }

    @Test
    public void testConfigureResetsCacheWhitelistAndPolicy() {
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(createCatalogContext("read", "data", "bucket-0"));

        verifyCacheFlags(fileIO, false, true, false);
        assertThat(fileIO.shouldCache(DATA_PATH)).isTrue();
        assertThat(fileIO.shouldCache(META_PATH)).isFalse();

        fileIO.configure(createCatalogContext("read", "meta", "*"));

        verifyCacheFlags(fileIO, false, true, false);
        assertThat(fileIO.shouldCache(DATA_PATH)).isFalse();
        assertThat(fileIO.shouldCache(META_PATH)).isTrue();

        fileIO.configure(createCatalogContext("none", "data", "bucket-0"));

        verifyCacheFlags(fileIO, false, false, false);
        assertThat(fileIO.shouldCache(DATA_PATH)).isFalse();
    }

    @Test
    public void testSerialization() throws Exception {
        CatalogContext context = createCatalogContext("read", "data", "bucket-0");
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(context);

        JindoFileIO cloned = InstantiationUtil.clone(fileIO);

        assertThat(cloned.shouldCache(DATA_PATH)).isTrue();
        assertThat(cloned.shouldCache(OTHER_DATA_PATH)).isFalse();
        assertThat(cloned.shouldCache(META_PATH)).isFalse();
        assertThat(cloned.hadoopOptions(DATA_PATH, "read").get("fs.oss.endpoint"))
                .isEqualTo("test-dlf-cache-endpoint");
        assertThat(cloned.hadoopOptions(META_PATH, "read").get("fs.oss.endpoint"))
                .isEqualTo("test-oss-endpoint");
    }
}
