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

package org.apache.paimon.flink.factories;

import org.apache.paimon.flink.log.LogStoreTableFactory;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.ManagedTableFactory.DEFAULT_IDENTIFIER;

/** Utility for working with {@link LogStoreTableFactory}s. */
public final class LogStoreFactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(LogStoreFactoryUtil.class);

    /** Discovers a LogStoreTableFactory using the given factory base class and identifier. */
    @SuppressWarnings("unchecked")
    public static <T extends LogStoreTableFactory> T discoverLogStoreFactory(
            ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
        final List<LogStoreTableFactory> factories = discoverLogStoreFactories(classLoader);

        final List<LogStoreTableFactory> foundFactories =
                factories.stream()
                        .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        if (foundFactories.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not find any factories that implement '%s' in the classpath.",
                            factoryClass.getName()));
        }

        final List<LogStoreTableFactory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equals(factoryIdentifier))
                        .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factory identifiers are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            foundFactories.stream()
                                    .map(LogStoreTableFactory::factoryIdentifier)
                                    .filter(identifier -> !DEFAULT_IDENTIFIER.equals(identifier))
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingFactories.size() > 1) {
            throw new ValidationException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return (T) matchingFactories.get(0);
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    static List<LogStoreTableFactory> discoverLogStoreFactories(ClassLoader classLoader) {
        final Iterator<LogStoreTableFactory> serviceLoaderIterator =
                ServiceLoader.load(LogStoreTableFactory.class, classLoader).iterator();

        final List<LogStoreTableFactory> loadResults = new ArrayList<>();
        while (true) {
            try {
                // error handling should also be applied to the hasNext() call because service
                // loading might cause problems here as well
                if (!serviceLoaderIterator.hasNext()) {
                    break;
                }

                loadResults.add(serviceLoaderIterator.next());
            } catch (Throwable t) {
                if (t instanceof NoClassDefFoundError) {
                    LOG.debug(
                            "NoClassDefFoundError when loading a "
                                    + LogStoreTableFactory.class.getCanonicalName()
                                    + ". This is expected when trying to load a format dependency but no flink-connector-files is loaded.",
                            t);
                } else {
                    throw new TableException(
                            "Unexpected error when trying to load service provider.", t);
                }
            }
        }

        return loadResults;
    }

    // --------------------------------------------------------------------------------------------

    private LogStoreFactoryUtil() {
        // no instantiation
    }
}
