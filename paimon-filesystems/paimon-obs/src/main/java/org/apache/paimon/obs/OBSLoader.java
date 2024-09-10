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

package org.apache.paimon.obs;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PluginFileIO;
import org.apache.paimon.plugin.PluginLoader;

import java.util.ArrayList;
import java.util.List;

/** A {@link PluginLoader} to load obs. */
public class OBSLoader implements FileIOLoader {

    private static final String OBS_JAR = "paimon-plugin-obs";

    private static final String OBS_CLASS = "org.apache.paimon.obs.OBSFileIO";

    // Singleton lazy initialization

    private static PluginLoader loader;

    private static synchronized PluginLoader getLoader() {
        if (loader == null) {
            // Avoid NoClassDefFoundError without cause by exception
            loader = new PluginLoader(OBS_JAR);
        }
        return loader;
    }

    @Override
    public String getScheme() {
        return "obs";
    }

    @Override
    public List<String[]> requiredOptions() {
        List<String[]> options = new ArrayList<>();
        options.add(new String[] {"fs.obs.endpoint"});
        options.add(new String[] {"fs.obs.access.key"});
        options.add(new String[] {"fs.obs.secret.key"});
        return options;
    }

    @Override
    public FileIO load(Path path) {
        return new OBSPluginFileIO();
    }

    private static class OBSPluginFileIO extends PluginFileIO {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        protected FileIO createFileIO(Path path) {
            FileIO fileIO = getLoader().newInstance(OBS_CLASS);
            fileIO.configure(CatalogContext.create(options));
            return fileIO;
        }

        @Override
        protected ClassLoader pluginClassLoader() {
            return getLoader().submoduleClassLoader();
        }
    }
}
