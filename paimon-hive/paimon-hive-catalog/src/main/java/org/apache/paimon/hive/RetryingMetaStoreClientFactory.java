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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.utils.StringUtils.isNullOrWhitespaceOnly;

/** Factory to create {@link RetryingMetaStoreClient}. */
public class RetryingMetaStoreClientFactory {

    private static final Map<Class<?>[], HiveMetastoreProxySupplier> PROXY_SUPPLIERS =
            ImmutableMap.<Class<?>[], HiveMetastoreProxySupplier>builder()
                    // for hive 1.x
                    .put(
                            new Class<?>[] {
                                HiveConf.class,
                                HiveMetaHookLoader.class,
                                ConcurrentHashMap.class,
                                String.class
                            },
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient)
                                            getProxyMethod.invoke(
                                                    null,
                                                    hiveConf,
                                                    (HiveMetaHookLoader) (tbl -> null),
                                                    new ConcurrentHashMap<>(),
                                                    clientClassName))
                    // for hive 2.x
                    .put(
                            new Class<?>[] {
                                HiveConf.class,
                                HiveMetaHookLoader.class,
                                ConcurrentHashMap.class,
                                String.class,
                                Boolean.TYPE
                            },
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient)
                                            getProxyMethod.invoke(
                                                    null,
                                                    hiveConf,
                                                    (HiveMetaHookLoader) (tbl -> null),
                                                    new ConcurrentHashMap<>(),
                                                    clientClassName,
                                                    true))
                    .put(
                            new Class<?>[] {
                                HiveConf.class,
                                Class[].class,
                                Object[].class,
                                ConcurrentHashMap.class,
                                String.class
                            },
                            RetryingMetaStoreClientFactory
                                    ::constructorDetectedHiveMetastoreProxySupplier)
                    // for hive 3.x
                    .put(
                            new Class<?>[] {
                                Configuration.class,
                                HiveMetaHookLoader.class,
                                ConcurrentHashMap.class,
                                String.class,
                                Boolean.TYPE
                            },
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient)
                                            getProxyMethod.invoke(
                                                    null,
                                                    hiveConf,
                                                    (HiveMetaHookLoader) (tbl -> null),
                                                    new ConcurrentHashMap<>(),
                                                    clientClassName,
                                                    true))
                    // for hive 3.x,
                    // and some metastore client classes providing constructors only for 2.x
                    .put(
                            new Class<?>[] {
                                Configuration.class,
                                Class[].class,
                                Object[].class,
                                ConcurrentHashMap.class,
                                String.class
                            },
                            RetryingMetaStoreClientFactory
                                    ::constructorDetectedHiveMetastoreProxySupplier)
                    .build();

    // If clientClassName is HiveMetaStoreClient,
    // we can revert to the simplest creation method,
    // which allows us to use shaded Hive packages to avoid dependency conflicts,
    // such as using apache-hive2.jar in Presto and Trino.
    private static final Map<Class<?>[], HiveMetastoreProxySupplier> PROXY_SUPPLIERS_SHADED =
            ImmutableMap.<Class<?>[], HiveMetastoreProxySupplier>builder()
                    .put(
                            new Class<?>[] {HiveConf.class},
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient) getProxyMethod.invoke(null, hiveConf))
                    .put(
                            new Class<?>[] {HiveConf.class, Boolean.TYPE},
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient) getProxyMethod.invoke(null, hiveConf, true))
                    .put(
                            new Class<?>[] {Configuration.class, Boolean.TYPE},
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient) getProxyMethod.invoke(null, hiveConf, true))
                    .build();

    public IMetaStoreClient createClient(HiveConf hiveConf, String clientClassName) {
        Map<Class<?>[], HiveMetastoreProxySupplier> suppliers =
                new LinkedHashMap<>(PROXY_SUPPLIERS);
        if (HiveMetaStoreClient.class.getName().equals(clientClassName)) {
            suppliers.putAll(PROXY_SUPPLIERS_SHADED);
        }

        RuntimeException failToCreate =
                new RuntimeException(
                        "Failed to create the desired metastore client (class name: "
                                + clientClassName
                                + ")");
        for (Entry<Class<?>[], HiveMetastoreProxySupplier> entry : suppliers.entrySet()) {
            Class<?>[] classes = entry.getKey();
            try {
                Method getProxy = RetryingMetaStoreClient.class.getMethod("getProxy", classes);
                HiveMetastoreProxySupplier supplier = entry.getValue();
                IMetaStoreClient client = supplier.get(getProxy, hiveConf, clientClassName);
                return isNullOrWhitespaceOnly(hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname))
                        ? client
                        : HiveMetaStoreClient.newSynchronizedClient(client);
            } catch (Exception e) {
                failToCreate.addSuppressed(e);
            }
        }
        throw failToCreate;
    }

    /** Function interface for creating hive metastore proxy. */
    public interface HiveMetastoreProxySupplier {
        IMetaStoreClient get(Method getProxyMethod, Configuration conf, String clientClassName)
                throws IllegalAccessException, IllegalArgumentException, InvocationTargetException;
    }

    /** Detect the client class whether it has the proper constructor. */
    private static IMetaStoreClient constructorDetectedHiveMetastoreProxySupplier(
            Method getProxyMethod, Configuration hiveConf, String clientClassName)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        try {
            Class<?> baseClass = Class.forName(clientClassName, false, JavaUtils.getClassLoader());

            // Configuration.class or HiveConf.class
            Class<?> firstParamType = getProxyMethod.getParameterTypes()[0];

            Class<?>[] fullParams =
                    new Class[] {firstParamType, HiveMetaHookLoader.class, Boolean.TYPE};
            Object[] fullParamValues =
                    new Object[] {hiveConf, (HiveMetaHookLoader) (tbl -> null), Boolean.TRUE};

            for (int i = fullParams.length; i >= 1; i--) {
                try {
                    baseClass.getConstructor(Arrays.copyOfRange(fullParams, 0, i));
                    return (IMetaStoreClient)
                            getProxyMethod.invoke(
                                    null,
                                    hiveConf,
                                    Arrays.copyOfRange(fullParams, 0, i),
                                    Arrays.copyOfRange(fullParamValues, 0, i),
                                    new ConcurrentHashMap<>(),
                                    clientClassName);
                } catch (NoSuchMethodException ignored) {
                }
            }

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        throw new IllegalArgumentException(
                "Failed to create the desired metastore client with proper constructors (class name: "
                        + clientClassName
                        + ")");
    }
}
