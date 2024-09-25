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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test Hive Client Pool.
 *
 * <p>Mostly copied from iceberg.
 */
public class TestHiveClientPool {

    private static final String HIVE_SITE_CONTENT =
            "<?xml version=\"1.0\"?>\n"
                    + "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"
                    + "<configuration>\n"
                    + "  <property>\n"
                    + "    <name>hive.metastore.sasl.enabled</name>\n"
                    + "    <value>true</value>\n"
                    + "  </property>\n"
                    + "</configuration>\n";

    HiveClientPool clients;

    @BeforeEach
    public void before() {
        String metastoreClientClass = "org.apache.hadoop.hive.metastore.HiveMetaStoreClient";
        HiveClientPool clientPool =
                new HiveClientPool(2, new Configuration(), metastoreClientClass);
        clients = Mockito.spy(clientPool);
    }

    @AfterEach
    public void after() {
        clients.close();
        clients = null;
    }

    @Test
    public void testGetTablesFails() throws Exception {
        HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);
        Mockito.doThrow(new MetaException("Another meta exception"))
                .when(hmsClient)
                .getTables(Mockito.anyString(), Mockito.anyString());
        clients.clients().clear();
        clients.clients().add(hmsClient);

        assertThatThrownBy(() -> clients.run(client -> client.getTables("default", "t")))
                .isInstanceOf(MetaException.class)
                .hasMessage("Another meta exception");
    }
}
