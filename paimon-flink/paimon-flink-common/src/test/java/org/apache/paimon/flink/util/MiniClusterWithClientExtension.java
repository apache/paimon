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

package org.apache.paimon.flink.util;

import org.apache.paimon.annotation.Experimental;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.testutils.InternalMiniClusterExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.function.Supplier;

/** Mini cluster extension with cluster client which is used to cancel jobs. */
@Experimental
public final class MiniClusterWithClientExtension
        implements BeforeAllCallback,
                BeforeEachCallback,
                AfterEachCallback,
                AfterAllCallback,
                ParameterResolver {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(MiniClusterWithClientExtension.class);

    private static final String CLUSTER_REST_CLIENT = "clusterRestClient";
    private static final String MINI_CLUSTER_CLIENT = "miniClusterClient";

    private final Supplier<MiniClusterResourceConfiguration>
            miniClusterResourceConfigurationSupplier;

    private InternalMiniClusterExtension internalMiniClusterExtension;

    public MiniClusterWithClientExtension(
            final MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
        this(() -> miniClusterResourceConfiguration);
    }

    @Experimental
    public MiniClusterWithClientExtension(
            Supplier<MiniClusterResourceConfiguration> miniClusterResourceConfigurationSupplier) {
        this.miniClusterResourceConfigurationSupplier = miniClusterResourceConfigurationSupplier;
    }

    // Accessors

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        Class<?> parameterType = parameterContext.getParameter().getType();
        if (parameterContext.isAnnotated(InjectClusterClient.class)
                && ClusterClient.class.isAssignableFrom(parameterType)) {
            return true;
        }
        return internalMiniClusterExtension.supportsParameter(parameterContext, extensionContext);
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        Class<?> parameterType = parameterContext.getParameter().getType();
        if (parameterContext.isAnnotated(InjectClusterClient.class)) {
            if (parameterType.equals(RestClusterClient.class)) {
                return extensionContext
                        .getStore(NAMESPACE)
                        .getOrComputeIfAbsent(
                                CLUSTER_REST_CLIENT,
                                k -> {
                                    try {
                                        return new CloseableParameter<>(
                                                createRestClusterClient(
                                                        internalMiniClusterExtension));
                                    } catch (Exception e) {
                                        throw new ParameterResolutionException(
                                                "Cannot create rest cluster client", e);
                                    }
                                },
                                CloseableParameter.class)
                        .get();
            }
            // Default to MiniClusterClient
            return extensionContext
                    .getStore(NAMESPACE)
                    .getOrComputeIfAbsent(
                            MINI_CLUSTER_CLIENT,
                            k -> {
                                try {
                                    return new CloseableParameter<>(
                                            createMiniClusterClient(internalMiniClusterExtension));
                                } catch (Exception e) {
                                    throw new ParameterResolutionException(
                                            "Cannot create mini cluster client", e);
                                }
                            },
                            CloseableParameter.class)
                    .get();
        }
        return internalMiniClusterExtension.resolveParameter(parameterContext, extensionContext);
    }

    // Lifecycle implementation

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        internalMiniClusterExtension =
                new InternalMiniClusterExtension(miniClusterResourceConfigurationSupplier.get());
        internalMiniClusterExtension.beforeAll(context);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        registerEnv(internalMiniClusterExtension);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        unregisterEnv(internalMiniClusterExtension);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (internalMiniClusterExtension != null) {
            internalMiniClusterExtension.afterAll(context);
        }
    }

    // Implementation

    private void registerEnv(InternalMiniClusterExtension internalMiniClusterExtension) {
        final Configuration configuration =
                internalMiniClusterExtension.getMiniCluster().getConfiguration();

        final int defaultParallelism =
                configuration
                        .getOptional(CoreOptions.DEFAULT_PARALLELISM)
                        .orElse(internalMiniClusterExtension.getNumberSlots());

        TestStreamEnvironment.setAsContext(
                internalMiniClusterExtension.getMiniCluster(), defaultParallelism);
    }

    private void unregisterEnv(InternalMiniClusterExtension internalMiniClusterExtension) {
        TestStreamEnvironment.unsetAsContext();
    }

    private MiniClusterClient createMiniClusterClient(
            InternalMiniClusterExtension internalMiniClusterExtension) {
        return new MiniClusterClient(
                internalMiniClusterExtension.getClientConfiguration(),
                internalMiniClusterExtension.getMiniCluster());
    }

    private RestClusterClient<MiniClusterClient.MiniClusterId> createRestClusterClient(
            InternalMiniClusterExtension internalMiniClusterExtension) throws Exception {
        return new RestClusterClient<>(
                internalMiniClusterExtension.getClientConfiguration(),
                MiniClusterClient.MiniClusterId.INSTANCE);
    }

    public RestClusterClient<MiniClusterClient.MiniClusterId> createRestClusterClient()
            throws Exception {
        return createRestClusterClient(internalMiniClusterExtension);
    }

    // Utils

    public Configuration getClientConfiguration() {
        return internalMiniClusterExtension.getClientConfiguration();
    }

    private static class CloseableParameter<T extends AutoCloseable>
            implements ExtensionContext.Store.CloseableResource {
        private final T autoCloseable;

        CloseableParameter(T autoCloseable) {
            this.autoCloseable = autoCloseable;
        }

        public T get() {
            return autoCloseable;
        }

        @Override
        public void close() throws Throwable {
            this.autoCloseable.close();
        }
    }
}
