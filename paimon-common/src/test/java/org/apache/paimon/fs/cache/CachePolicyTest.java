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

package org.apache.paimon.fs.cache;

import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_IGNORE_VPC_VALIDATE;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_POLICY;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_VPC_VALIDATED;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_WHITELIST;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_WHITELIST_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for backend-independent REST cache policy semantics. */
class CachePolicyTest {

    private static final String ROOT = "oss://bucket/database/table";
    private static final Path DATA = new Path(ROOT + "/bucket-0/data-a1b2c3d4-0.orc");
    private static final Path META = new Path(ROOT + "/snapshot/snapshot-1");
    private static final Path MANIFEST = new Path(ROOT + "/manifest/manifest-a1b2c3d4-0");

    @Test
    void testOperationTagsAndCombinations() {
        for (String tags : new String[] {"meta", "read", "write", "meta,read,write"}) {
            CachePolicy policy = new CachePolicy(options(tags));
            for (String operation : new String[] {"meta", "read", "write"}) {
                assertThat(policy.shouldCache(DATA, operation))
                        .as("%s with %s", tags, operation)
                        .isEqualTo(tags.contains(operation));
            }
            assertThat(policy.shouldCache(DATA, "list")).isFalse();
            assertThat(policy.shouldCache(DATA, "delete")).isFalse();
        }
    }

    @Test
    void testDisabledPolicies() {
        for (String tags : new String[] {null, "none", "none,read"}) {
            Options options = options(tags);
            options.set(IO_CACHE_IGNORE_VPC_VALIDATE, true);
            CachePolicy policy = new CachePolicy(options);
            assertThat(policy.enabled()).isFalse();
            assertThat(policy.shouldCache(DATA, "read")).isFalse();
        }
    }

    @Test
    void testVpcValidationAndOverride() {
        Options options = options("read");
        options.remove(IO_CACHE_VPC_VALIDATED);
        assertThat(new CachePolicy(options).enabled()).isFalse();
        options.set(IO_CACHE_VPC_VALIDATED, false);
        assertThat(new CachePolicy(options).enabled()).isFalse();
        options.set(IO_CACHE_IGNORE_VPC_VALIDATE, true);
        assertThat(new CachePolicy(options).shouldCache(DATA, "read")).isTrue();
        options.set(IO_CACHE_IGNORE_VPC_VALIDATE, false);
        options.set(IO_CACHE_VPC_VALIDATED, true);
        assertThat(new CachePolicy(options).shouldCache(DATA, "read")).isTrue();
    }

    @Test
    void testDefaultAndExplicitFileTypes() {
        Path[] paths = {
            META,
            MANIFEST,
            DATA,
            new Path(ROOT + "/index/btree-global-index-a1b2c3d4.index"),
            new Path(ROOT + "/index/index-a1b2c3d4-0"),
            new Path(ROOT + "/bucket-0/data-a1b2c3d4-0.orc.index")
        };
        Options options = options("read");
        for (String whitelist :
                new String[] {"*", "meta,global-index,bucket-index,data,file-index"}) {
            options.set(IO_CACHE_WHITELIST, whitelist);
            CachePolicy policy = new CachePolicy(options);
            for (Path path : paths) {
                assertThat(policy.shouldCache(path, "read")).as("%s", path).isTrue();
            }
        }
        options.set(IO_CACHE_WHITELIST, "data,file-index");
        assertThat(new CachePolicy(options).shouldCache(META, "read")).isFalse();
        assertThat(new CachePolicy(options).shouldCache(DATA, "read")).isTrue();
    }

    @Test
    void testPathAndTypeWhitelistMustBothMatch() {
        Options options = options("read");
        options.set(IO_CACHE_WHITELIST_PATH, " manifest, bucket-0, ");
        CachePolicy policy = new CachePolicy(options);
        assertThat(policy.shouldCache(MANIFEST, "read")).isTrue();
        assertThat(policy.shouldCache(DATA, "read")).isTrue();
        assertThat(policy.shouldCache(META, "read")).isFalse();
        assertThat(policy.shouldCache(new Path(ROOT + "/bucket-1/data.orc"), "read")).isFalse();
        options.set(IO_CACHE_WHITELIST, "meta");
        policy = new CachePolicy(options);
        assertThat(policy.shouldCache(MANIFEST, "read")).isTrue();
        assertThat(policy.shouldCache(DATA, "read")).isFalse();
    }

    @Test
    void testEmptyAndUnknownWhitelistsFailClosed() {
        Options options = options("read");
        for (String whitelist : new String[] {"", "manifest,unknown"}) {
            options.set(IO_CACHE_WHITELIST, whitelist);
            assertThat(new CachePolicy(options).shouldCache(DATA, "read")).isFalse();
            assertThat(new CachePolicy(options).shouldCache(META, "read")).isFalse();
        }
        options.set(IO_CACHE_WHITELIST, "*");
        options.set(IO_CACHE_WHITELIST_PATH, " , ");
        assertThat(new CachePolicy(options).shouldCache(DATA, "read")).isFalse();
    }

    @Test
    void testPathMatchingExcludesSchemeAndAuthority() {
        Options options = options("read");
        for (String pattern : new String[] {"oss", "bucket/", "?bucket-0"}) {
            options.set(IO_CACHE_WHITELIST_PATH, pattern);
            assertThat(new CachePolicy(options).shouldCache(DATA, "read")).isFalse();
        }
        options.set(IO_CACHE_WHITELIST_PATH, " /database/table/bucket-0 ");
        assertThat(new CachePolicy(options).shouldCache(DATA, "READ")).isTrue();
    }

    @Test
    void testMutableFilesBypassAllOperations() {
        CachePolicy policy = new CachePolicy(options("meta,read,write"));
        for (String suffix :
                new String[] {
                    "/snapshot/LATEST", "/snapshot/EARLIEST", "/consumer/consumer-user"
                }) {
            for (String operation : new String[] {"meta", "read", "write"}) {
                assertThat(policy.shouldCache(new Path(ROOT + suffix), operation)).isFalse();
            }
        }
        assertThat(policy.shouldCache(META, "meta")).isTrue();
    }

    @Test
    void testPolicyIsASerializableSnapshot() throws Exception {
        Options options = options("read");
        options.set(IO_CACHE_WHITELIST_PATH, "bucket-0");
        CachePolicy policy = InstantiationUtil.clone(new CachePolicy(options));
        options.set(IO_CACHE_POLICY, "none");
        options.set(IO_CACHE_WHITELIST_PATH, "*");
        assertThat(policy.shouldCache(DATA, "read")).isTrue();
        assertThat(policy.shouldCache(META, "read")).isFalse();
    }

    private static Options options(String tags) {
        Options options = new Options();
        if (tags != null) {
            options.set(IO_CACHE_POLICY, tags);
        }
        options.set(IO_CACHE_VPC_VALIDATED, true);
        return options;
    }
}
