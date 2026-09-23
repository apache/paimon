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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.Identifier;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HiveAlterTableUtils}. */
public class HiveAlterTableUtilsTest {

    @Test
    public void testAlterTableWithoutCascade() throws Exception {
        CapturingMetaStoreClient handler = new CapturingMetaStoreClient(false);
        IMetaStoreClient client = handler.client();
        Identifier identifier = Identifier.create("db", "tbl");
        Table table = new Table();
        table.setParameters(new HashMap<>());

        HiveAlterTableUtils.alterTable(client, identifier, table, true, false);

        assertThat(handler.environmentContext.getProperties())
                .containsEntry(StatsSetupConst.DO_NOT_UPDATE_STATS, "true")
                .doesNotContainKey(StatsSetupConst.CASCADE);
    }

    @Test
    public void testAlterTableWithCascade() throws Exception {
        CapturingMetaStoreClient handler = new CapturingMetaStoreClient(false);
        IMetaStoreClient client = handler.client();
        Identifier identifier = Identifier.create("db", "tbl");
        Table table = new Table();
        table.setParameters(new HashMap<>());

        HiveAlterTableUtils.alterTable(client, identifier, table, true, true);

        assertThat(handler.environmentContext.getProperties())
                .containsEntry(StatsSetupConst.DO_NOT_UPDATE_STATS, "true")
                .containsEntry(StatsSetupConst.CASCADE, "true");
    }

    @Test
    public void testAlterTableWithoutEnvUsesConfiguredCascade() throws Exception {
        CapturingMetaStoreClient handler = new CapturingMetaStoreClient(true);
        IMetaStoreClient client = handler.client();
        Identifier identifier = Identifier.create("db", "tbl");
        Table table = new Table();
        table.setParameters(new HashMap<>());

        HiveAlterTableUtils.alterTable(client, identifier, table, true, false);

        assertThat(handler.cascade).isFalse();
    }

    private static class CapturingMetaStoreClient implements InvocationHandler {

        private final boolean failWithEnv;
        private EnvironmentContext environmentContext;
        private Boolean cascade;

        private CapturingMetaStoreClient(boolean failWithEnv) {
            this.failWithEnv = failWithEnv;
        }

        private IMetaStoreClient client() {
            return (IMetaStoreClient)
                    Proxy.newProxyInstance(
                            IMetaStoreClient.class.getClassLoader(),
                            new Class[] {IMetaStoreClient.class},
                            this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if ("alter_table_with_environmentContext".equals(method.getName())) {
                if (failWithEnv) {
                    throw new NoSuchMethodError();
                }
                environmentContext = (EnvironmentContext) args[3];
                return null;
            }
            if ("alter_table".equals(method.getName()) && args.length == 4) {
                cascade = (Boolean) args[3];
                return null;
            }
            return defaultValue(method.getReturnType());
        }

        private Object defaultValue(Class<?> returnType) {
            if (!returnType.isPrimitive() || Void.TYPE.equals(returnType)) {
                return null;
            }
            if (Boolean.TYPE.equals(returnType)) {
                return false;
            }
            if (Character.TYPE.equals(returnType)) {
                return '\0';
            }
            if (Byte.TYPE.equals(returnType)) {
                return (byte) 0;
            }
            if (Short.TYPE.equals(returnType)) {
                return (short) 0;
            }
            if (Integer.TYPE.equals(returnType)) {
                return 0;
            }
            if (Long.TYPE.equals(returnType)) {
                return 0L;
            }
            if (Float.TYPE.equals(returnType)) {
                return 0F;
            }
            return 0D;
        }
    }
}
