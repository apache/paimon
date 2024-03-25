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

package org.apache.paimon.plugin;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Loader to load plugin jar. */
public class PluginLoader {

    public static final String[] PARENT_FIRST_LOGGING_PATTERNS =
            new String[] {
                "org.slf4j",
                "org.apache.log4j",
                "org.apache.logging",
                "org.apache.commons.logging",
                "ch.qos.logback"
            };

    private static final String[] OWNER_CLASSPATH =
            Stream.concat(
                            Arrays.stream(PARENT_FIRST_LOGGING_PATTERNS),
                            Stream.of(
                                    "javax.xml.bind",
                                    "org.codehaus.janino",
                                    "org.codehaus.commons",
                                    "org.apache.commons.lang3"))
                    .toArray(String[]::new);

    private static final String[] COMPONENT_CLASSPATH = new String[] {"org.apache.paimon"};

    private final ComponentClassLoader submoduleClassLoader;

    public PluginLoader(String dirName) {
        // URL must end with slash,
        // otherwise URLClassLoader cannot recognize this path as a directory
        if (!dirName.endsWith("/")) {
            dirName += "/";
        }

        ClassLoader ownerClassLoader = PluginLoader.class.getClassLoader();
        this.submoduleClassLoader =
                new ComponentClassLoader(
                        new URL[] {ownerClassLoader.getResource(dirName)},
                        ownerClassLoader,
                        OWNER_CLASSPATH,
                        COMPONENT_CLASSPATH);
    }

    public <T> T discover(Class<T> clazz) {
        List<T> results = new ArrayList<>();
        ServiceLoader.load(clazz, submoduleClassLoader).iterator().forEachRemaining(results::add);
        if (results.size() != 1) {
            throw new RuntimeException(
                    "Found "
                            + results.size()
                            + " classes implementing "
                            + clazz.getName()
                            + ". They are:\n"
                            + results.stream()
                                    .map(t -> t.getClass().getName())
                                    .collect(Collectors.joining("\n")));
        }
        return results.get(0);
    }

    @SuppressWarnings("unchecked")
    public <T> T newInstance(String name) {
        try {
            return (T) submoduleClassLoader.loadClass(name).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public ClassLoader submoduleClassLoader() {
        return submoduleClassLoader;
    }
}
