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

package org.apache.paimon.flink.utils;

import org.apache.paimon.plugin.ComponentClassLoader;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

/** calcite Module. */
public class CalciteModule {
    private static final String SQL_BASIC_CALL_GET_OPERATOR_METHOD = "getOperator";

    static final String FLINK_TABLE_PLANNER_FAT_JAR = "flink-table-planner.jar";

    private static final String HINT_USAGE =
            "mvn clean package -pl flink-table/flink-table-planner,flink-table/flink-table-planner-loader -DskipTests";

    private static final String[] OWNER_CLASSPATH =
            Stream.concat(
                            Arrays.stream(CoreOptions.PARENT_FIRST_LOGGING_PATTERNS),
                            Stream.of(
                                    // These packages are shipped either by
                                    // flink-table-runtime or flink-dist itself
                                    "org.codehaus.janino",
                                    "org.codehaus.commons",
                                    "org.apache.commons.lang3",
                                    "org.apache.commons.math3",
                                    "org.apache.paimon",
                                    // with hive dialect, hadoop jar should be in classpath,
                                    // also, we should make it loaded by owner classloader,
                                    // otherwise, it'll throw class not found exception
                                    // when initialize HiveParser which requires hadoop
                                    "org.apache.hadoop"))
                    .toArray(String[]::new);

    private static final String[] COMPONENT_CLASSPATH = new String[] {"org.apache.flink"};

    private final ComponentClassLoader submoduleClassLoader;

    public CalciteModule() {
        try {
            final ClassLoader flinkClassLoader = CalciteModule.class.getClassLoader();

            final Path tmpDirectory =
                    Paths.get(ConfigurationUtils.parseTempDirectories(new Configuration())[0]);
            Files.createDirectories(FileUtils.getTargetPathIfContainsSymbolicPath(tmpDirectory));
            final Path tempFile =
                    Files.createFile(
                            tmpDirectory.resolve(
                                    "flink-table-planner_" + UUID.randomUUID() + ".jar"));

            final InputStream resourceStream =
                    flinkClassLoader.getResourceAsStream(FLINK_TABLE_PLANNER_FAT_JAR);
            if (resourceStream == null) {
                throw new TableException(
                        String.format(
                                "Flink Table planner could not be found. If this happened while running a test in the IDE, "
                                        + "run '%s' on the command-line, "
                                        + "or add a test dependency on the flink-table-planner-loader test-jar.",
                                HINT_USAGE));
            }

            IOUtils.copyBytes(resourceStream, Files.newOutputStream(tempFile));
            tempFile.toFile().deleteOnExit();

            this.submoduleClassLoader =
                    new ComponentClassLoader(
                            new URL[] {tempFile.toUri().toURL()},
                            flinkClassLoader,
                            OWNER_CLASSPATH,
                            COMPONENT_CLASSPATH);
        } catch (IOException e) {
            throw new TableException(
                    "Could not initialize the table planner components loader.", e);
        }
    }

    public Predicate getPredicate(String whereSql, RowType rowType) throws Exception {
        Class<?> clazz =
                Class.forName(
                        "org.apache.paimon.flink.predicate.SimpleSqlPredicateConvertor",
                        true,
                        submoduleClassLoader);
        ClassLoader preClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(submoduleClassLoader);
            Constructor<?> constructor = clazz.getConstructor(RowType.class);
            // convertor
            Object o = constructor.newInstance(rowType);

            // convert
            Method method = clazz.getMethod("convertSqlToPredicate", String.class);
            return (Predicate) method.invoke(o, whereSql);
        } finally {
            Thread.currentThread().setContextClassLoader(preClassLoader);
        }
    }
}
