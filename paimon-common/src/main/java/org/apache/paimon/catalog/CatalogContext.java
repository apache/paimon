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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.utils.HadoopUtils.getHadoopConfiguration;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Context of catalog.
 *
 * @since 0.4.0
 */
@Public
public class CatalogContext {

    private final Options options;
    private final Configuration hadoopConf;
    @Nullable private final FileIOLoader preferIOLoader;
    @Nullable private final FileIOLoader fallbackIOLoader;

    private CatalogContext(
            Options options,
            @Nullable Configuration hadoopConf,
            @Nullable FileIOLoader preferIOLoader,
            @Nullable FileIOLoader fallbackIOLoader) {
        this.options = checkNotNull(options);
        this.hadoopConf = hadoopConf == null ? getHadoopConfiguration(options) : hadoopConf;
        this.preferIOLoader = preferIOLoader;
        this.fallbackIOLoader = fallbackIOLoader;
    }

    public static CatalogContext create(Path warehouse) {
        Options options = new Options();
        options.set(WAREHOUSE, warehouse.toUri().toString());
        return create(options);
    }

    public static CatalogContext create(Options options) {
        return new CatalogContext(options, null, null, null);
    }

    public static CatalogContext create(Options options, Configuration hadoopConf) {
        return new CatalogContext(options, hadoopConf, null, null);
    }

    public static CatalogContext create(Options options, FileIOLoader fallbackIOLoader) {
        return new CatalogContext(options, null, null, fallbackIOLoader);
    }

    public static CatalogContext create(
            Options options, FileIOLoader preferIOLoader, FileIOLoader fallbackIOLoader) {
        return new CatalogContext(options, null, preferIOLoader, fallbackIOLoader);
    }

    public static CatalogContext create(
            Options options,
            Configuration hadoopConf,
            FileIOLoader preferIOLoader,
            FileIOLoader fallbackIOLoader) {
        return new CatalogContext(options, hadoopConf, preferIOLoader, fallbackIOLoader);
    }

    public Options options() {
        return options;
    }

    /** Return hadoop {@link Configuration}. */
    public Configuration hadoopConf() {
        return hadoopConf;
    }

    @Nullable
    public FileIOLoader preferIO() {
        return preferIOLoader;
    }

    @Nullable
    public FileIOLoader fallbackIO() {
        return fallbackIOLoader;
    }
}
