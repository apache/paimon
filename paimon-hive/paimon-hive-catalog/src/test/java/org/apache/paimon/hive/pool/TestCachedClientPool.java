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

package org.apache.paimon.hive.pool;

import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.AuthorizationMetaStoreFilterHook;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CachedClientPool}. */
public class TestCachedClientPool {

    @Test
    public void testCacheKeyNotSame() {
        // client1 use cache key type:paimon
        Options options1 = new Options();
        options1.set("type", "paimon");
        options1.set("paimon.catalog.type", "hive");
        options1.set("hive.metastore.uris", "thrift://localhost:9083");
        options1.set("client-pool-cache.keys", "conf:type");
        CachedClientPool cache1 =
                new CachedClientPool(
                        new Configuration(), options1, HiveMetaStoreClient.class.getName());

        // client2 use cache key type:paimon
        Options options2 = new Options();
        options2.set("type", "paimon");
        options2.set("paimon.catalog.type", "hive");
        options2.set("hive.metastore.uris", "thrift://localhost:9083");
        options2.set("client-pool-cache.keys", "conf:paimon.catalog.type");
        CachedClientPool cache2 =
                new CachedClientPool(
                        new Configuration(), options2, HiveMetaStoreClient.class.getName());

        // assert return the different object instance
        assertThat(cache1.clientPool()).isNotEqualTo(cache2.clientPool());
    }

    @Test
    public void testCacheKeySame() {
        Options options = new Options();
        options.set("type", "paimon");
        options.set("paimon.catalog.type", "hive");
        options.set("hive.metastore.uris", "thrift://localhost:9083");
        options.set("client-pool-cache.keys", "conf:type");

        // client1 & client2 use the same cache key paimon.catalog.type:hive
        CachedClientPool cache1 =
                new CachedClientPool(
                        new Configuration(), options, HiveMetaStoreClient.class.getName());
        CachedClientPool cache2 =
                new CachedClientPool(
                        new Configuration(), options, HiveMetaStoreClient.class.getName());

        // assert return the same object instance
        assertThat(cache1.clientPool()).isEqualTo(cache2.clientPool());
    }

    @Test
    public void testCacheKeyUGINotSame() {
        // user paimon1 login
        Options options1 = new Options();
        options1.set("type", "paimon");
        options1.set("paimon.catalog.type", "hive");
        options1.set("hive.metastore.uris", "thrift://localhost:9083");
        options1.set("client-pool-cache.keys", "ugi,conf:type");

        CachedClientPool cache1 =
                UserGroupInformation.createRemoteUser("paimon1")
                        .doAs(
                                (PrivilegedAction<CachedClientPool>)
                                        () ->
                                                new CachedClientPool(
                                                        new Configuration(),
                                                        options1,
                                                        HiveMetaStoreClient.class.getName()));

        // user paimon2 login
        Options options2 = new Options();
        options2.set("type", "paimon");
        options2.set("paimon.catalog.type", "hive");
        options2.set("hive.metastore.uris", "thrift://localhost:9083");
        options2.set("client-pool-cache.keys", "ugi,conf:type");

        CachedClientPool cache2 =
                UserGroupInformation.createRemoteUser("paimon2")
                        .doAs(
                                (PrivilegedAction<CachedClientPool>)
                                        () ->
                                                new CachedClientPool(
                                                        new Configuration(),
                                                        options2,
                                                        HiveMetaStoreClient.class.getName()));

        assertThat(cache1.clientPool()).isNotEqualTo(cache2.clientPool());
    }

    @Test
    public void testCacheKeyUGISame() {
        Options options = new Options();
        options.set("type", "paimon");
        options.set("paimon.catalog.type", "hive");
        options.set("hive.metastore.uris", "thrift://localhost:9083");
        options.set("client-pool-cache.keys", "ugi,conf:type");

        // user paimon login
        CachedClientPool cache1 =
                UserGroupInformation.createRemoteUser("paimon")
                        .doAs(
                                (PrivilegedAction<CachedClientPool>)
                                        () ->
                                                new CachedClientPool(
                                                        new Configuration(),
                                                        options,
                                                        HiveMetaStoreClient.class.getName()));

        // user paimon login again
        CachedClientPool cache2 =
                UserGroupInformation.createRemoteUser("paimon")
                        .doAs(
                                (PrivilegedAction<CachedClientPool>)
                                        () ->
                                                new CachedClientPool(
                                                        new Configuration(),
                                                        options,
                                                        HiveMetaStoreClient.class.getName()));

        assertThat(cache1.clientPool()).isEqualTo(cache2.clientPool());
    }

    @Test
    public void testCacheKeyOptionNotSame() {
        // client1 use option1 instance
        Options options1 = new Options();
        options1.set("type", "paimon");
        options1.set("paimon.catalog.type", "hive");
        options1.set("hive.metastore.uris", "thrift://localhost:9083");
        options1.set("client-pool-cache.keys", "conf:*");
        CachedClientPool cache1 =
                new CachedClientPool(
                        new Configuration(), options1, HiveMetaStoreClient.class.getName());

        // client1 use option2 instance
        Options options2 = new Options();
        options2.set("type", "hive");
        options2.set("paimon.catalog.type", "hive");
        options2.set("hive.metastore.uris", "thrift://localhost:9083");
        options1.set("client-pool-cache.keys", "conf:*");
        CachedClientPool cache2 =
                new CachedClientPool(
                        new Configuration(), options2, HiveMetaStoreClient.class.getName());

        // assert return the different object instance
        assertThat(cache1.clientPool()).isNotEqualTo(cache2.clientPool());
    }

    @Test
    public void testCacheKeyOptionSame1() {
        // need use the same option instance
        Options options = new Options();
        options.set("type", "paimon");
        options.set("paimon.catalog.type", "hive");
        options.set("hive.metastore.uris", "thrift://localhost:9083");
        options.set("client-pool-cache.keys", "conf:*");

        // client1 & client2 use the same option instance
        CachedClientPool cache1 =
                new CachedClientPool(
                        new Configuration(), options, HiveMetaStoreClient.class.getName());
        CachedClientPool cache2 =
                new CachedClientPool(
                        new Configuration(), options, HiveMetaStoreClient.class.getName());

        // assert return the same object instance
        assertThat(cache1.clientPool()).isEqualTo(cache2.clientPool());
    }

    @Test
    public void testCacheKeyOptionSame2() {
        // client1 use option1 instance
        Options options1 = new Options();
        options1.set("type", "paimon");
        options1.set("paimon.catalog.type", "hive");
        options1.set("hive.metastore.uris", "thrift://localhost:9083");
        options1.set("client-pool-cache.keys", "conf:*");
        CachedClientPool cache1 =
                new CachedClientPool(
                        new Configuration(), options1, HiveMetaStoreClient.class.getName());

        // client1 use option2 instance
        Options options2 = new Options();
        options2.set("type", "paimon");
        options2.set("paimon.catalog.type", "hive");
        options2.set("hive.metastore.uris", "thrift://localhost:9083");
        options2.set("client-pool-cache.keys", "conf:*");
        CachedClientPool cache2 =
                new CachedClientPool(
                        new Configuration(), options2, HiveMetaStoreClient.class.getName());

        // assert return the same object instance
        assertThat(cache1.clientPool()).isEqualTo(cache2.clientPool());
    }

    @Test
    public void testCacheKeyOptionAllNotSame() {
        // user paimon1 login
        Options options1 = new Options();
        options1.set("type", "paimon");
        options1.set("paimon.catalog.type", "hive");
        options1.set("hive.metastore.uris", "thrift://localhost:9083");
        options1.set("client-pool-cache.keys", "ugi,conf:*");

        CachedClientPool cache1 =
                UserGroupInformation.createRemoteUser("paimon1")
                        .doAs(
                                (PrivilegedAction<CachedClientPool>)
                                        () ->
                                                new CachedClientPool(
                                                        new Configuration(),
                                                        options1,
                                                        HiveMetaStoreClient.class.getName()));

        // user paimon2 login
        Options options2 = new Options();
        options2.set("type", "hive");
        options2.set("paimon.catalog.type", "hive");
        options2.set("hive.metastore.uris", "thrift://localhost:9083");
        options2.set("client-pool-cache.keys", "ugi,conf:*");

        CachedClientPool cache2 =
                UserGroupInformation.createRemoteUser("paimon2")
                        .doAs(
                                (PrivilegedAction<CachedClientPool>)
                                        () ->
                                                new CachedClientPool(
                                                        new Configuration(),
                                                        options2,
                                                        HiveMetaStoreClient.class.getName()));

        // assert return the different object instance
        assertThat(cache1.clientPool()).isNotEqualTo(cache2.clientPool());
    }

    @Test
    public void testCacheKeyOptionAllSame() {
        Options options = new Options();
        options.set("type", "paimon");
        options.set("paimon.catalog.type", "hive");
        options.set("hive.metastore.uris", "thrift://localhost:9083");
        options.set("client-pool-cache.keys", "ugi,conf:type");

        // user paimon login
        CachedClientPool cache1 =
                UserGroupInformation.createRemoteUser("paimon")
                        .doAs(
                                (PrivilegedAction<CachedClientPool>)
                                        () ->
                                                new CachedClientPool(
                                                        new Configuration(),
                                                        options,
                                                        HiveMetaStoreClient.class.getName()));

        // user paimon login again
        CachedClientPool cache2 =
                UserGroupInformation.createRemoteUser("paimon")
                        .doAs(
                                (PrivilegedAction<CachedClientPool>)
                                        () ->
                                                new CachedClientPool(
                                                        new Configuration(),
                                                        options,
                                                        HiveMetaStoreClient.class.getName()));

        assertThat(cache1.clientPool()).isEqualTo(cache2.clientPool());
    }

    @Test
    public void testLoginPaimon() throws TException {
        // user paimon login
        Options options = new Options();
        options.set("type", "paimon");
        options.set("paimon.catalog.type", "hive");
        options.set("hive.metastore.uris", "thrift://30.150.24.155:9083");
        options.set("client-pool-cache.keys", "ugi,conf:*");
        options.set(
                "hive.metastore.filter.hook", MockAuthorizationMetaStoreFilterHook.class.getName());

        Configuration config = new Configuration();
        config.set("current.user", "paimon");
        config.set(
                "hive.metastore.filter.hook", MockAuthorizationMetaStoreFilterHook.class.getName());
        CachedClientPool cache =
                UserGroupInformation.createRemoteUser("paimon")
                        .doAs(
                                (PrivilegedAction<CachedClientPool>)
                                        () ->
                                                new CachedClientPool(
                                                        config,
                                                        options,
                                                        HiveMetaStoreClient.class.getName()));

        // contain default database
        assertThat(cache.clientPool().newClient().getAllDatabases()).contains("default");
    }

    @Test
    public void testLoginRoot() throws TException {
        // user paimon login
        Options options = new Options();
        options.set("type", "paimon");
        options.set("paimon.catalog.type", "hive");
        options.set("hive.metastore.uris", "thrift://30.150.24.155:9083");
        options.set("client-pool-cache.keys", "ugi,conf:*");

        Configuration config = new Configuration();
        config.set("current.user", "root");
        config.set(
                "hive.metastore.filter.hook", MockAuthorizationMetaStoreFilterHook.class.getName());
        CachedClientPool cache =
                UserGroupInformation.createRemoteUser("root")
                        .doAs(
                                (PrivilegedAction<CachedClientPool>)
                                        () ->
                                                new CachedClientPool(
                                                        config,
                                                        options,
                                                        HiveMetaStoreClient.class.getName()));

        // contain default database
        assertThat(cache.clientPool().newClient().getAllDatabases()).contains("default");
    }

    /** Tests for {@link CachedClientPool}. */
    public static class MockAuthorizationMetaStoreFilterHook
            extends AuthorizationMetaStoreFilterHook {
        HiveConf conf;
        UserGroupInformation ugi;

        public MockAuthorizationMetaStoreFilterHook(HiveConf conf) throws IOException {
            super(conf);
            this.conf = conf;
            this.ugi = UserGroupInformation.getCurrentUser();
            this.checkCurrentUser(this.conf);
        }

        public List<String> filterDatabases(List<String> dbList) throws MetaException {
            return this.ugi.doAs(
                    (PrivilegedAction<List<String>>)
                            () -> {
                                this.checkCurrentUser(this.conf);
                                return dbList;
                            });
        }

        private void checkCurrentUser(HiveConf conf) {
            try {
                assertThat(UserGroupInformation.getCurrentUser().getUserName())
                        .isEqualTo(conf.get("current.user"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
