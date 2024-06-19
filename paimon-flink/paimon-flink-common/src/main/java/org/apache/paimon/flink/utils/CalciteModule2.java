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

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.util.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/** calcite Module. */
public class CalciteModule2 {
    private static final String Flink_PLANNER_MODULE_CLASS =
            "org.apache.flink.table.planner.loader.PlannerModule";
    private static final String PLANNER_MODULE_METHOD = "getInstance";

    private static final String SUBMODULE_CLASS_LOADER = "submoduleClassLoader";

    private final ClassLoader classLoader;

    public CalciteModule2() throws Exception {

        final Path tmpDirectory =
                Paths.get(ConfigurationUtils.parseTempDirectories(new Configuration())[0]);
        Files.createDirectories(FileUtils.getTargetPathIfContainsSymbolicPath(tmpDirectory));
        Path path = Files.createFile(tmpDirectory.resolve("paimon_" + UUID.randomUUID() + ".jar"));
        try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(path))) {
            addClassToJar(jos, "org.apache.paimon.flink.predicate.SimpleSqlPredicateConvertor");
        }

        classLoader = initCalciteClassLoader();
        Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        method.setAccessible(true);
        method.invoke(classLoader, path.toUri().toURL());
    }

    private static void addClassToJar(JarOutputStream jos, String className) throws IOException {

        String classPath = className.replace('.', '/') + ".class";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        JarEntry entry = new JarEntry(classPath);
        try (InputStream resourceAsStream = loader.getResourceAsStream(classPath)) {
            if (resourceAsStream == null) {
                throw new IOException("Could not find the class file for " + className);
            }
            jos.putNextEntry(entry);
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = resourceAsStream.read(buffer)) != -1) {
                jos.write(buffer, 0, bytesRead);
            }
            jos.closeEntry();
        }
    }

    private ClassLoader initCalciteClassLoader() throws Exception {
        Class<?> plannerModuleClass = Class.forName(Flink_PLANNER_MODULE_CLASS);
        Method getInstanceMethod = plannerModuleClass.getDeclaredMethod(PLANNER_MODULE_METHOD);
        getInstanceMethod.setAccessible(true);
        Object plannerModuleInstance = getInstanceMethod.invoke(null);

        Field submoduleClassLoaderField =
                plannerModuleClass.getDeclaredField(SUBMODULE_CLASS_LOADER);
        submoduleClassLoaderField.setAccessible(true);
        return (ClassLoader) submoduleClassLoaderField.get(plannerModuleInstance);
    }

    public Predicate getPredicate(String whereSql, RowType rowType) throws Exception {
        Class<?> clazz =
                Class.forName(
                        "org.apache.paimon.flink.predicate.SimpleSqlPredicateConvertor",
                        true,
                        classLoader);
        ClassLoader preClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
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
