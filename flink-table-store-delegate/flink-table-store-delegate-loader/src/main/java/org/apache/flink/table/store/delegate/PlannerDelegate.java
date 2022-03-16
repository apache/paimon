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

package org.apache.flink.table.store.delegate;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.classloading.ComponentClassLoader;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Copied and modified from the flink-table-planner-loader module. */
public class PlannerDelegate {

    /**
     * The name of the table planner dependency jar, bundled with flink-table-planner-loader module
     * artifact.
     */
    static final String FLINK_TABLE_PLANNER_FAT_JAR = "flink-table-planner.jar";

    static final String FLINK_TABLE_STORE_DELEGATE_PLANNER_FAT_JAR =
            "flink-table-store-delegate-planner.jar";

    private static final String[] OWNER_CLASSPATH =
            Stream.concat(
                            Arrays.stream(CoreOptions.PARENT_FIRST_LOGGING_PATTERNS),
                            Stream.of(
                                    // These packages are shipped either by
                                    // flink-table-runtime or flink-dist itself
                                    "org.codehaus.janino",
                                    "org.codehaus.commons",
                                    "org.apache.commons.lang3"))
                    .toArray(String[]::new);

    private static final String[] COMPONENT_CLASSPATH = new String[] {"org.apache.flink"};

    private static final Map<String, String> KNOWN_MODULE_ASSOCIATIONS = new HashMap<>();

    static {
        KNOWN_MODULE_ASSOCIATIONS.put("org.apache.flink.table.runtime", "flink-table-runtime");
        KNOWN_MODULE_ASSOCIATIONS.put("org.apache.flink.formats.raw", "flink-table-runtime");

        KNOWN_MODULE_ASSOCIATIONS.put("org.codehaus.janino", "flink-table-runtime");
        KNOWN_MODULE_ASSOCIATIONS.put("org.codehaus.commons", "flink-table-runtime");
        KNOWN_MODULE_ASSOCIATIONS.put(
                "org.apache.flink.table.shaded.com.jayway", "flink-table-runtime");
    }

    private final ClassLoader submoduleClassLoader;

    private PlannerDelegate() {
        try {
            final ClassLoader flinkClassLoader = PlannerDelegate.class.getClassLoader();
            final Path tmpDirectory =
                    Paths.get(ConfigurationUtils.parseTempDirectories(new Configuration())[0]);
            Files.createDirectories(tmpDirectory);

            Path plannerJar =
                    extractResource(
                            FLINK_TABLE_PLANNER_FAT_JAR,
                            flinkClassLoader,
                            tmpDirectory,
                            "Flink table planner could not be found.\n"
                                    + "If you're running a test, please add the following dependency to your pom.xml.\n"
                                    + "<dependency>\n"
                                    + "    <groupId>org.apache.flink</groupId>\n"
                                    + "    <artifactId>flink-table-planner-loader</artifactId>\n"
                                    + "    <version>${flink.version}</version>\n"
                                    + "    <scope>test</scope>\n"
                                    + "</dependency>");
            Path delegateJar =
                    extractResource(
                            FLINK_TABLE_STORE_DELEGATE_PLANNER_FAT_JAR,
                            flinkClassLoader,
                            tmpDirectory,
                            "Flink table store delegate planner could not be found.\n"
                                    + "Please make sure you've built the delegate modules by running\n"
                                    + "cd flink-table-store-delegate && mvn clean package -DskipTests\n"
                                    + "If you're running a test, please add the following dependency to your pom.xml.\n"
                                    + "<dependency>\n"
                                    + "    <groupId>org.apache.flink</groupId>\n"
                                    + "    <artifactId>flink-table-store-delegate-loader</artifactId>\n"
                                    + "    <version>${project.version}</version>\n"
                                    + "    <scope>test</scope>\n"
                                    + "</dependency>");

            this.submoduleClassLoader =
                    new ComponentClassLoader(
                            new URL[] {plannerJar.toUri().toURL(), delegateJar.toUri().toURL()},
                            flinkClassLoader,
                            OWNER_CLASSPATH,
                            COMPONENT_CLASSPATH,
                            KNOWN_MODULE_ASSOCIATIONS);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Could not initialize the table planner components loader.", e);
        }
    }

    private Path extractResource(
            String resourceName,
            ClassLoader flinkClassLoader,
            Path tmpDirectory,
            String errorMessage)
            throws IOException {
        String[] splitName = resourceName.split("\\.");
        splitName[0] += "_" + UUID.randomUUID();
        final Path tempFile = Files.createFile(tmpDirectory.resolve(String.join(".", splitName)));
        final InputStream resourceStream = flinkClassLoader.getResourceAsStream(resourceName);
        if (resourceStream == null) {
            throw new RuntimeException(errorMessage);
        }
        IOUtils.copyBytes(resourceStream, Files.newOutputStream(tempFile));
        return tempFile;
    }

    // Singleton lazy initialization

    private static class PlannerComponentsHolder {
        private static final PlannerDelegate INSTANCE = new PlannerDelegate();
    }

    public static PlannerDelegate getInstance() {
        return PlannerComponentsHolder.INSTANCE;
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
}
