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

package org.apache.flink.table.store.s3;

import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.table.store.plugin.FileSystemLoader;
import org.apache.flink.table.store.plugin.PluginLoader;

/** A {@link PluginLoader} to load oss. */
public class S3Loader implements FileSystemLoader {

    private static final String S3_JAR = "flink-table-store-plugin-s3.jar";

    private static final String S3_CLASS = "org.apache.flink.fs.s3hadoop.S3FileSystemFactory";

    // Singleton lazy initialization

    private static PluginLoader loader;

    private static synchronized PluginLoader getLoader() {
        if (loader == null) {
            // Avoid NoClassDefFoundError without cause by exception
            loader = new PluginLoader(S3_JAR);
        }
        return loader;
    }

    @Override
    public FileSystemFactory load() {
        return getLoader().newInstance(S3_CLASS);
    }
}
