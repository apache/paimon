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

package org.apache.paimon.catalog;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.CatalogTableType;
import org.apache.paimon.utils.HadoopUtilsITCase.TestFileIOLoader;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import static org.apache.paimon.options.CatalogOptions.TABLE_TYPE;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CatalogFactory}. */
public class CatalogFactoryTest {

    @Test
    public void testAutomaticCreatePath(@TempDir java.nio.file.Path path) {
        Path root = new Path(path.toUri().toString());
        Options options = new Options();
        options.set(WAREHOUSE, new Path(root, "warehouse").toString());
        assertThat(CatalogFactory.createCatalog(CatalogContext.create(options)).listDatabases())
                .isEmpty();
    }

    @Test
    public void testNotDirectory(@TempDir java.nio.file.Path path) throws IOException {
        Path root = new Path(path.toUri().toString());
        Path warehouse = new Path(root, "warehouse");
        LocalFileIO.create().tryToWriteAtomic(warehouse, "");
        Options options = new Options();
        options.set(WAREHOUSE, warehouse.toString());
        assertThatThrownBy(() -> CatalogFactory.createCatalog(CatalogContext.create(options)))
                .hasMessageContaining("should be a directory");
    }

    @Test
    public void testNonManagedTable(@TempDir java.nio.file.Path path) {
        Path root = new Path(path.toUri().toString());
        Options options = new Options();
        options.set(WAREHOUSE, new Path(root, "warehouse").toString());
        options.set(TABLE_TYPE, CatalogTableType.EXTERNAL);
        assertThatThrownBy(() -> CatalogFactory.createCatalog(CatalogContext.create(options)))
                .hasMessageContaining("Only managed table is supported in File system catalog.");
    }

    @Test
    public void testContextDefaultHadoopConf(@TempDir java.nio.file.Path path) {
        Path root = new Path(path.toUri().toString());
        String defaultFS = "master:9999";
        String replication = "8";

        Options options = new Options();
        options.set(WAREHOUSE, new Path(root, "warehouse").toString());
        options.set("hadoop.fs.defaultFS", defaultFS);
        options.set("hadoop.dfs.replication", replication);
        Configuration conf = CatalogContext.create(options).hadoopConf();

        assertThat(conf).isInstanceOf(HdfsConfiguration.class);
        assertThat(conf.get("fs.defaultFS")).isEqualTo(defaultFS);
        assertThat(conf.get("dfs.replication")).isEqualTo(replication);
    }

    @Test
    public void testCreateCatalogWithoutHadoop(@TempDir java.nio.file.Path path) {
        Path root = new Path(path.toUri().toString());
        Options options = new Options();
        options.set(WAREHOUSE, new Path(root, "warehouse").toString());

        CatalogContext context =
                CatalogContext.createWithoutHadoop(
                        options, new TraceableFileIO.Loader(), null);

        assertThat(CatalogFactory.createCatalog(context).listDatabases()).isEmpty();
        assertThatThrownBy(context::hadoopConf)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Hadoop configuration is not available");
    }

    @Test
    public void testCreateCatalogWithoutHadoopClasses(@TempDir java.nio.file.Path path)
            throws Exception {
        try (URLClassLoader classLoader = new NoHadoopClassLoader(testClasspathWithoutHadoop())) {
            Class<?> runner =
                    Class.forName(
                            NoHadoopCatalogContextRunner.class.getName(), true, classLoader);
            runner.getMethod("run", String.class, ClassLoader.class)
                    .invoke(null, new Path(path.toUri().toString(), "warehouse").toString(), classLoader);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw (Error) cause;
        }
    }

    @Test
    public void testContextSerializable() throws IOException, ClassNotFoundException {
        Configuration conf = new Configuration(false);
        conf.set("my_key", "my_value");
        CatalogContext context =
                CatalogContext.create(
                        new Options(), conf, new TestFileIOLoader(), new TestFileIOLoader());
        context = InstantiationUtil.clone(context);
        assertThat(context.hadoopConf().get("my_key")).isEqualTo(conf.get("my_key"));
    }

    private static URL[] testClasspathWithoutHadoop() {
        return Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
                .filter(path -> !path.contains("/hadoop-"))
                .filter(path -> !path.contains("/htrace-core"))
                .filter(path -> !path.contains("/woodstox-core"))
                .filter(path -> !path.contains("/stax2-api"))
                .map(
                        path -> {
                            try {
                                return new File(path).toURI().toURL();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .toArray(URL[]::new);
    }

    private static class NoHadoopClassLoader extends URLClassLoader {

        private NoHadoopClassLoader(URL[] urls) {
            super(urls, ClassLoader.getSystemClassLoader().getParent());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("org.apache.hadoop.")) {
                throw new ClassNotFoundException(name);
            }
            return super.loadClass(name, resolve);
        }
    }

    /** Runner loaded by {@link NoHadoopClassLoader} to verify no-Hadoop catalog creation. */
    public static class NoHadoopCatalogContextRunner {

        public static void run(String warehouse, ClassLoader classLoader) throws Exception {
            assertThatThrownBy(() -> classLoader.loadClass("org.apache.hadoop.conf.Configuration"))
                    .isInstanceOf(ClassNotFoundException.class);

            Options options = new Options();
            options.set("warehouse", warehouse);
            CatalogContext context =
                    CatalogContext.createWithoutHadoop(
                            options, new TraceableFileIO.Loader(), null);
            Catalog catalog = CatalogFactory.createCatalog(context, classLoader);

            assertThat(catalog.listDatabases()).isEmpty();
        }
    }
}
