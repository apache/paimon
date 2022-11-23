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

package org.apache.flink.table.store.codegen;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.table.store.utils.LocalFileUtils;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Copied and modified from the flink-table-planner-loader module. */
public class CodeGenLoader {

    static final String FLINK_TABLE_STORE_CODEGEN_FAT_JAR = "flink-table-store-codegen.jar";

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
                                    // These packages are shipped either by
                                    // flink-table-runtime or flink-dist itself
                                    "org.codehaus.janino",
                                    "org.codehaus.commons",
                                    "org.apache.commons.lang3"))
                    .toArray(String[]::new);

    private static final String[] COMPONENT_CLASSPATH = new String[] {"org.apache.flink"};

    private final ClassLoader submoduleClassLoader;

    private CodeGenLoader() {
        try {
            final ClassLoader flinkClassLoader = CodeGenLoader.class.getClassLoader();
            final Path tmpDirectory =
                    Paths.get(ConfigurationUtils.parseTempDirectories(new Configuration())[0]);
            Files.createDirectories(
                    LocalFileUtils.getTargetPathIfContainsSymbolicPath(tmpDirectory));
            Path delegateJar =
                    extractResource(
                            FLINK_TABLE_STORE_CODEGEN_FAT_JAR,
                            flinkClassLoader,
                            tmpDirectory,
                            "Flink table store codegen could not be found.\n"
                                    + "If you're running a test, please make sure you've built the codegen modules by running\n"
                                    + "mvn clean package -pl flink-table-store-codegen,flink-table-store-codegen-loader -DskipTests");
            this.submoduleClassLoader =
                    new ComponentClassLoader(
                            new URL[] {delegateJar.toUri().toURL()},
                            flinkClassLoader,
                            OWNER_CLASSPATH,
                            COMPONENT_CLASSPATH);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Could not initialize the flink-table-store-codegen loader.", e);
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

    private static CodeGenLoader instance;

    public static synchronized CodeGenLoader getInstance() {
        if (instance == null) {
            // Avoid NoClassDefFoundError without cause by exception
            instance = new CodeGenLoader();
        }
        return instance;
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
