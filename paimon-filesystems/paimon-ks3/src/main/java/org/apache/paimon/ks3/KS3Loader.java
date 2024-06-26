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

package org.apache.paimon.ks3;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PluginFileIO;
import org.apache.paimon.plugin.PluginLoader;

import java.util.ArrayList;
import java.util.List;

/** A {@link PluginLoader} to load ks3. */
public class KS3Loader implements FileIOLoader {

    private static final String KS3_CLASSES_DIR = "paimon-plugin-ks3";

    private static final String KS3_CLASS = "org.apache.paimon.ks3.KS3FileIO";

    // Singleton lazy initialization

    private static PluginLoader loader;

    private static synchronized PluginLoader getLoader() {
        if (loader == null) {
            // Avoid NoClassDefFoundError without cause by exception
            loader = new PluginLoader(KS3_CLASSES_DIR);
        }
        return loader;
    }

    @Override
    public String getScheme() {
        return "ks3";
    }

    @Override
    public List<String[]> requiredOptions() {
        List<String[]> options = new ArrayList<>();
        options.add(new String[] {"fs.ks3.AccessKey"});
        options.add(new String[] {"fs.ks3.AccessSecret"});
        options.add(new String[] {"fs.ks3.endpoint"});
        return options;
    }

    @Override
    public FileIO load(Path path) {
        return new KS3PluginFileIO();
    }

    private static class KS3PluginFileIO extends PluginFileIO {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        protected FileIO createFileIO(Path path) {
            FileIO fileIO = getLoader().newInstance(KS3_CLASS);
            fileIO.configure(CatalogContext.create(options));
            return fileIO;
        }

        @Override
        protected ClassLoader pluginClassLoader() {
            return getLoader().submoduleClassLoader();
        }
    }
}
