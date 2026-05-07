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

package org.apache.paimon.elasticsearch.index.util;

import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.logging.internal.spi.LoggerFactory;

/**
 * SLF4J-bridging {@link LoggerFactory} for running ES vector index code outside the full
 * Elasticsearch runtime. Delegates all ES logging calls to SLF4J so that logs appear in the Paimon
 * logging framework. Registers itself on first use via {@link #ensureInitialized()}.
 */
public class Slf4jBridgeESLoggerFactory extends LoggerFactory {

    private static volatile boolean initialized;

    public static void ensureInitialized() {
        if (!initialized) {
            synchronized (Slf4jBridgeESLoggerFactory.class) {
                if (!initialized) {
                    LoggerFactory.setInstance(new Slf4jBridgeESLoggerFactory());
                    initialized = true;
                }
            }
        }
    }

    @Override
    public Logger getLogger(String name) {
        return new Slf4jBridgeLogger(org.slf4j.LoggerFactory.getLogger(name));
    }

    @Override
    public Logger getLogger(Class<?> clazz) {
        return new Slf4jBridgeLogger(org.slf4j.LoggerFactory.getLogger(clazz));
    }

    @Override
    public void setRootLevel(Level level) {}

    @Override
    public Level getRootLevel() {
        return Level.INFO;
    }
}
