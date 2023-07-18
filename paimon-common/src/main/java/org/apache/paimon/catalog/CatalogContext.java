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
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.lineage.LineageMeta;
import org.apache.paimon.lineage.LineageMetaFactory;
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import static org.apache.paimon.options.CatalogOptions.LINEAGE_META;
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
    @Nullable private final FileIOLoader fallbackIOLoader;
    @Nullable private final LineageMeta lineageMeta;

    private CatalogContext(
            Options options,
            @Nullable Configuration hadoopConf,
            @Nullable FileIOLoader fallbackIOLoader,
            @Nullable LineageMeta lineageMeta) {
        this.options = checkNotNull(options);
        this.hadoopConf = hadoopConf == null ? getHadoopConfiguration(options) : hadoopConf;
        this.fallbackIOLoader = fallbackIOLoader;
        this.lineageMeta = lineageMeta;
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

    public static CatalogContext create(
            Options options, FileIOLoader fallbackIOLoader, ClassLoader classLoader) {
        return new CatalogContext(
                options, null, fallbackIOLoader, findAndCreateLineageMeta(options, classLoader));
    }

    public static CatalogContext create(
            Options options, Configuration hadoopConf, FileIOLoader fallbackIOLoader) {
        return new CatalogContext(options, hadoopConf, fallbackIOLoader, null);
    }

    @Nullable
    private static LineageMeta findAndCreateLineageMeta(Options options, ClassLoader classLoader) {
        return options.getOptional(LINEAGE_META)
                .map(
                        meta ->
                                FactoryUtil.discoverFactory(
                                                classLoader, LineageMetaFactory.class, meta)
                                        .create(() -> options))
                .orElse(null);
    }

    public Options options() {
        return options;
    }

    /** Return hadoop {@link Configuration}. */
    public Configuration hadoopConf() {
        return hadoopConf;
    }

    @Nullable
    public FileIOLoader fallbackIO() {
        return fallbackIOLoader;
    }

    @Nullable
    public LineageMeta lineageMeta() {
        return lineageMeta;
    }
}
