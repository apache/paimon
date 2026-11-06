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

package org.apache.paimon.fileindex;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests index discovery with blind and child context classloaders. */
public class IndexerFactoryUtilsClassLoaderTest {

    @Test
    public void testFileIndexFactoriesIgnoreContextClassLoader() throws Exception {
        Object factory =
                loadWithBlindContextClassLoader(
                        FileIndexerFactoryUtils.class.getName(), "bloom-filter");
        assertThat(factory.getClass().getName())
                .isEqualTo("org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndexFactory");
    }

    @Test
    public void testGlobalIndexFactoriesIgnoreContextClassLoader() throws Exception {
        Object factory =
                loadWithBlindContextClassLoader(GlobalIndexerFactoryUtils.class.getName(), "btree");
        assertThat(factory.getClass().getName())
                .isEqualTo("org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory");
    }

    @Test
    public void testFileIndexFactoriesFromChildContextClassLoader(@TempDir Path tempDir)
            throws Exception {
        Object factory =
                loadWithChildContextClassLoader(
                        FileIndexerFactoryUtils.class.getName(),
                        ChildFileIndexerFactory.IDENTIFIER,
                        FileIndexerFactory.class,
                        ChildFileIndexerFactory.class,
                        tempDir);
        assertThat(factory.getClass().getName()).isEqualTo(ChildFileIndexerFactory.class.getName());
    }

    @Test
    public void testGlobalIndexFactoriesFromChildContextClassLoader(@TempDir Path tempDir)
            throws Exception {
        Object factory =
                loadWithChildContextClassLoader(
                        GlobalIndexerFactoryUtils.class.getName(),
                        ChildGlobalIndexerFactory.IDENTIFIER,
                        GlobalIndexerFactory.class,
                        ChildGlobalIndexerFactory.class,
                        tempDir);
        assertThat(factory.getClass().getName())
                .isEqualTo(ChildGlobalIndexerFactory.class.getName());
    }

    @Test
    public void testChildFileIndexRegistration(@TempDir Path tempDir) throws Exception {
        writeService(FileIndexerFactory.class, ChildFileIndexerFactory.class, tempDir);
        Thread thread = Thread.currentThread();
        ClassLoader original = thread.getContextClassLoader();
        try (URLClassLoader child =
                new URLClassLoader(
                        new URL[] {tempDir.toUri().toURL()}, getClass().getClassLoader())) {
            thread.setContextClassLoader(child);
            assertThat(FileIndexerFactoryUtils.isRegistered(ChildFileIndexerFactory.IDENTIFIER))
                    .isTrue();
            assertThat(FileIndexerFactoryUtils.isRegistered("missing-index")).isFalse();
            thread.setContextClassLoader(FileIndexerFactory.class.getClassLoader());
            assertThat(FileIndexerFactoryUtils.isRegistered(ChildFileIndexerFactory.IDENTIFIER))
                    .isFalse();
        } finally {
            thread.setContextClassLoader(original);
        }
    }

    @Test
    public void testChildGlobalIndexFileSelection(@TempDir Path tempDir) throws Exception {
        writeService(GlobalIndexerFactory.class, ChildGlobalIndexerFactory.class, tempDir);
        List<GlobalIndexIOMeta> files = Collections.singletonList(null);
        Thread thread = Thread.currentThread();
        ClassLoader original = thread.getContextClassLoader();
        try (URLClassLoader child =
                new URLClassLoader(
                        new URL[] {tempDir.toUri().toURL()}, getClass().getClassLoader())) {
            thread.setContextClassLoader(child);
            assertThat(
                            GlobalIndexerFactoryUtils.selectFiles(
                                    ChildGlobalIndexerFactory.IDENTIFIER,
                                    Collections.emptyList(),
                                    null,
                                    files))
                    .isEmpty();
            assertThat(
                            GlobalIndexerFactoryUtils.selectFiles(
                                    "missing-index", Collections.emptyList(), null, files))
                    .isSameAs(files);
            thread.setContextClassLoader(GlobalIndexerFactory.class.getClassLoader());
            assertThat(
                            GlobalIndexerFactoryUtils.selectFiles(
                                    ChildGlobalIndexerFactory.IDENTIFIER,
                                    Collections.emptyList(),
                                    null,
                                    files))
                    .isSameAs(files);
        } finally {
            thread.setContextClassLoader(original);
        }
    }

    // Initializes the utils class in a fresh classloader, as a Flink JobMaster RPC thread would.
    private static Object loadWithBlindContextClassLoader(String utilsClass, String type)
            throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader original = thread.getContextClassLoader();
        try (URLClassLoader isolated =
                        new URLClassLoader(
                                classPath(), ClassLoader.getSystemClassLoader().getParent());
                URLClassLoader blind = new URLClassLoader(new URL[0], null)) {
            thread.setContextClassLoader(blind);
            Class<?> utils = Class.forName(utilsClass, true, isolated);
            return utils.getMethod("load", String.class).invoke(null, type);
        } finally {
            thread.setContextClassLoader(original);
        }
    }

    private static Object loadWithChildContextClassLoader(
            String utilsClass, String type, Class<?> service, Class<?> provider, Path tempDir)
            throws Exception {
        copyProviderClass(provider, tempDir);
        writeService(service, provider, tempDir);

        Thread thread = Thread.currentThread();
        ClassLoader original = thread.getContextClassLoader();
        try (URLClassLoader isolated =
                        new URLClassLoader(
                                classPath(), ClassLoader.getSystemClassLoader().getParent());
                URLClassLoader blind = new URLClassLoader(new URL[0], null);
                URLClassLoader child =
                        new URLClassLoader(new URL[] {tempDir.toUri().toURL()}, isolated);
                URLClassLoader sibling =
                        new URLClassLoader(new URL[] {tempDir.toUri().toURL()}, isolated)) {
            assertThatThrownBy(() -> isolated.loadClass(provider.getName()))
                    .isInstanceOf(ClassNotFoundException.class);
            thread.setContextClassLoader(blind);
            Class<?> utils = Class.forName(utilsClass, true, isolated);
            assertThatThrownBy(() -> utils.getMethod("load", String.class).invoke(null, type))
                    .hasRootCauseMessage(
                            "Can't find "
                                    + (service == FileIndexerFactory.class ? "file" : "global")
                                    + " index for type: "
                                    + type);
            thread.setContextClassLoader(child);
            Object factory = utils.getMethod("load", String.class).invoke(null, type);
            assertThat(factory.getClass().getClassLoader()).isSameAs(child);
            thread.setContextClassLoader(sibling);
            Object siblingFactory = utils.getMethod("load", String.class).invoke(null, type);
            assertThat(siblingFactory.getClass().getClassLoader()).isSameAs(sibling);
            thread.setContextClassLoader(null);
            assertThatThrownBy(() -> utils.getMethod("load", String.class).invoke(null, type))
                    .hasRootCauseMessage(
                            "Can't find "
                                    + (service == FileIndexerFactory.class ? "file" : "global")
                                    + " index for type: "
                                    + type);
            return factory;
        } finally {
            thread.setContextClassLoader(original);
        }
    }

    private static void writeService(Class<?> service, Class<?> provider, Path tempDir)
            throws Exception {
        Path services = tempDir.resolve("META-INF/services");
        Files.createDirectories(services);
        Files.write(services.resolve(service.getName()), Collections.singleton(provider.getName()));
    }

    private static void copyProviderClass(Class<?> provider, Path tempDir) throws Exception {
        String resourceName = provider.getName().replace('.', '/') + ".class";
        Path classFile = tempDir.resolve(resourceName);
        Files.createDirectories(classFile.getParent());
        try (InputStream input = provider.getClassLoader().getResourceAsStream(resourceName)) {
            if (input == null) {
                throw new IllegalStateException("Cannot find provider class: " + resourceName);
            }
            Files.copy(input, classFile);
        }
    }

    /** Test factory loaded from a child classloader. */
    public static class ChildFileIndexerFactory implements FileIndexerFactory {

        private static final String IDENTIFIER = "child-file-index";

        @Override
        public String identifier() {
            return IDENTIFIER;
        }

        @Override
        public FileIndexer create(DataType type, Options options) {
            return null;
        }
    }

    /** Test factory loaded from a child classloader. */
    public static class ChildGlobalIndexerFactory implements GlobalIndexerFactory {

        private static final String IDENTIFIER = "child-global-index";

        @Override
        public String identifier() {
            return IDENTIFIER;
        }

        @Override
        public GlobalIndexer create(List<DataField> indexFields, Options options) {
            return null;
        }

        @Override
        public List<GlobalIndexIOMeta> selectFiles(
                List<DataField> indexFields, Predicate predicate, List<GlobalIndexIOMeta> files) {
            return Collections.emptyList();
        }
    }

    private static URL[] classPath() throws Exception {
        List<URL> urls = new ArrayList<>();
        URL testClasses =
                IndexerFactoryUtilsClassLoaderTest.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            URL url = new File(entry).toURI().toURL();
            if (!url.equals(testClasses)) {
                urls.add(url);
            }
        }
        return urls.toArray(new URL[0]);
    }
}
