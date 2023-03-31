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

package org.apache.paimon.flink.pulsar.sink;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalListener;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

/**
 * Enable the sharing of same PulsarClient among tasks in a same process.
 */
public class CachedPulsarClient {

    private static final Logger LOG = LoggerFactory.getLogger(CachedPulsarClient.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final RemovalListener<String, PulsarClientImpl> removalListener =
            notification -> {
                String config = notification.getKey();
                PulsarClientImpl client = notification.getValue();
                LOG.debug(
                        "Evicting pulsar client {} with config {}, due to {}",
                        client,
                        config,
                        notification.getCause());

                close(config, client);
            };
    private static int cacheSize = 100;
    private static final Cache<String, PulsarClientImpl> clientCache =
            CacheBuilder.newBuilder()
                    .maximumSize(cacheSize)
                    .removalListener(removalListener)
                    .build();

    public static int getCacheSize() {
        return cacheSize;
    }

    public static void setCacheSize(int newSize) {
        cacheSize = newSize;
    }

    private static PulsarClientImpl createPulsarClient(ClientConfigurationData clientConfig)
            throws PulsarClientException {
        PulsarClientImpl client;
        try {
            client = new PulsarClientImpl(clientConfig);
            LOG.debug(
                    "Created a new instance of PulsarClientImpl for clientConf = {}", clientConfig);
        } catch (PulsarClientException e) {
            LOG.error("Failed to create PulsarClientImpl for clientConf = {}", clientConfig);
            throw e;
        }
        return client;
    }

    public static synchronized PulsarClientImpl getOrCreate(ClientConfigurationData config)
            throws PulsarClientException {
        String key = null;
        try {
            key = serializeKey(config);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        PulsarClientImpl client = clientCache.getIfPresent(key);

        if (client == null) {
            client = createPulsarClient(config);
            clientCache.put(key, client);
        }

        return client;
    }

    private static void close(String clientConfig, PulsarClientImpl client) {
        if (client != null) {
            try {
                LOG.info("Closing the Pulsar client with config {}", clientConfig);
                client.close();
            } catch (PulsarClientException e) {
                LOG.warn(
                        String.format("Error while closing the Pulsar client %s", clientConfig), e);
            }
        }
    }

    private static String serializeKey(ClientConfigurationData clientConfig)
            throws JsonProcessingException {
        return mapper.writeValueAsString(clientConfig);
    }

    @VisibleForTesting
    static void close(ClientConfigurationData clientConfig) throws JsonProcessingException {
        String key = serializeKey(clientConfig);
        clientCache.invalidate(key);
    }

    @VisibleForTesting
    static void clear() {
        LOG.info("Cleaning up guava cache.");
        clientCache.invalidateAll();
    }

    @VisibleForTesting
    static ConcurrentMap<String, PulsarClientImpl> getAsMap() {
        return clientCache.asMap();
    }
}
