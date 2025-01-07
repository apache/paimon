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

package org.apache.paimon.factories;

import org.apache.paimon.format.FileFormatFactory;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.factories.FactoryUtil.discoverFactories;

/** Utility for working with {@link FileFormatFactory}s. */
public class FormatFactoryUtil {

    private static final Cache<ClassLoader, List<FileFormatFactory>> FACTORIES =
            Caffeine.newBuilder().softValues().maximumSize(100).executor(Runnable::run).build();

    /** Discovers a file format factory. */
    @SuppressWarnings("unchecked")
    public static <T extends FileFormatFactory> T discoverFactory(
            ClassLoader classLoader, String identifier) {
        final List<FileFormatFactory> foundFactories = getFactories(classLoader);

        final List<FileFormatFactory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.identifier().equals(identifier))
                        .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new FactoryException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements FileFormatFactory in the classpath.\n\n"
                                    + "Available factory identifiers are:\n\n"
                                    + "%s",
                            identifier,
                            foundFactories.stream()
                                    .map(FileFormatFactory::identifier)
                                    .collect(Collectors.joining("\n"))));
        }

        return (T) matchingFactories.get(0);
    }

    private static List<FileFormatFactory> getFactories(ClassLoader classLoader) {
        return FACTORIES.get(
                classLoader, s -> discoverFactories(classLoader, FileFormatFactory.class));
    }
}
