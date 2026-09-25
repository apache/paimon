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

package org.apache.paimon.flink;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link NestedFieldReferences}. */
public class NestedFieldReferencesTest {

    private static final String HIDDEN =
            "org.apache.flink.table.expressions.NestedFieldReferenceExpression";

    /**
     * Flink only has the nested field reference from 1.19 on, but this module is compiled once and
     * bundled for Flink 1.16 to 1.18 as well. Loaded where the class is absent, asking whether an
     * expression is a nested reference must answer no rather than fail to link.
     */
    @Test
    public void testAnswersNoWhenTheExpressionIsNotOnTheClasspath() throws Exception {
        try (URLClassLoader withoutNestedReferences = hidingClassLoader()) {
            Class<?> loaded =
                    withoutNestedReferences.loadClass(NestedFieldReferences.class.getName());
            assertThat(loaded.getClassLoader()).isSameAs(withoutNestedReferences);

            Method isNestedFieldReference =
                    loaded.getMethod(
                            "isNestedFieldReference",
                            withoutNestedReferences.loadClass(
                                    "org.apache.flink.table.expressions.Expression"));

            // A real expression, not null: `null instanceof X` answers without ever resolving X,
            // so only a non-null argument exercises the class the guard has to keep away.
            Object expression =
                    withoutNestedReferences
                            .loadClass("org.apache.flink.table.expressions.ValueLiteralExpression")
                            .getConstructor(Object.class)
                            .newInstance(1);

            assertThatCode(() -> isNestedFieldReference.invoke(null, expression))
                    .doesNotThrowAnyException();
            assertThat(isNestedFieldReference.invoke(null, expression)).isEqualTo(false);
        }
    }

    /** Loads this module's own classes itself, and pretends the nested reference does not exist. */
    private static URLClassLoader hidingClassLoader() {
        URLClassLoader appLoader = (URLClassLoader) buildClassPathLoader();
        return new URLClassLoader(appLoader.getURLs(), appLoader.getParent()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                    throws ClassNotFoundException {
                if (name.equals(HIDDEN)) {
                    throw new ClassNotFoundException(name);
                }
                if (name.startsWith("org.apache.paimon.flink.NestedFieldReferences")) {
                    synchronized (getClassLoadingLock(name)) {
                        Class<?> loaded = findLoadedClass(name);
                        if (loaded == null) {
                            loaded = findClass(name);
                        }
                        if (resolve) {
                            resolveClass(loaded);
                        }
                        return loaded;
                    }
                }
                return super.loadClass(name, resolve);
            }
        };
    }

    private static ClassLoader buildClassPathLoader() {
        String[] entries = System.getProperty("java.class.path").split(java.io.File.pathSeparator);
        URL[] urls = new URL[entries.length];
        for (int i = 0; i < entries.length; i++) {
            try {
                urls[i] = new java.io.File(entries[i]).toURI().toURL();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        // Not getPlatformClassLoader(): Paimon builds with Java 8, where it does not exist. The
        // system loader's parent serves the same purpose here - a parent without the classpath.
        return new URLClassLoader(urls, ClassLoader.getSystemClassLoader().getParent());
    }
}
