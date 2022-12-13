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

package org.apache.flink.table.store.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.table.store.plugin.FileSystemLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * A new {@link FileSystem} loader to support:
 *
 * <ul>
 *   <li>Support access to file systems in Hive, Spark, Trino and other computing engines.
 *   <li>TODO: Support access to multiple users' file systems in Flink clusters.
 * </ul>
 */
public class FileSystems {

    public static void initialize(Path path, Configuration configuration) {
        // 1. Try to load file system
        try {
            // check can obtain
            FileSystem fs = path.getFileSystem();
            // check can read
            fs.getFileStatus(path);
            return;
        } catch (IOException ignored) {
        }

        // 2. initialize
        FileSystem.initialize(
                configuration,
                new PluginManager() {
                    @Override
                    public <P> Iterator<P> load(Class<P> service) {
                        return (Iterator<P>) discoverFactories().iterator();
                    }
                });

        try {
            SecurityUtils.install(new SecurityConfiguration(configuration));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<FileSystemFactory> discoverFactories() {
        List<FileSystemFactory> results = new ArrayList<>();
        ServiceLoader.load(FileSystemLoader.class, FileSystemLoader.class.getClassLoader())
                .iterator()
                .forEachRemaining(loader -> results.add(loader.load()));
        return results;
    }
}
