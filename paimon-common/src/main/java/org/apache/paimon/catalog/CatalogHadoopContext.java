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
import org.apache.paimon.hadoop.SerializableConfiguration;
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.utils.HadoopUtils.getHadoopConfiguration;

/**
 * Context of catalog with Hadoop configuration support.
 *
 * <p>This class extends {@link CatalogContext} and implements {@link HadoopAware} to provide access
 * to Hadoop configuration. Use this class when Hadoop integration is required.
 *
 * @since 0.4.0
 */
@Public
public class CatalogHadoopContext extends CatalogContext implements HadoopAware {

    private static final long serialVersionUID = 1L;

    private final SerializableConfiguration hadoopConf;

    private CatalogHadoopContext(
            Options options,
            @Nullable Configuration hadoopConf,
            @Nullable FileIOLoader preferIOLoader,
            @Nullable FileIOLoader fallbackIOLoader) {
        super(options, preferIOLoader, fallbackIOLoader);
        this.hadoopConf =
                new SerializableConfiguration(
                        hadoopConf == null ? getHadoopConfiguration(options) : hadoopConf);
    }

    @Override
    public CatalogContext copy(Options options) {
        return CatalogHadoopContext.create(
                options, this.hadoopConf.get(), this.preferIOLoader, this.fallbackIOLoader);
    }

    @Override
    public CatalogContext copy(
            Options options, FileIOLoader preferIOLoader, FileIOLoader fileIOLoader) {
        return CatalogHadoopContext.create(options, preferIOLoader, fileIOLoader);
    }

    public static CatalogHadoopContext create(Path warehouse) {
        Options options = new Options();
        options.set(WAREHOUSE, warehouse.toUri().toString());
        return create(options);
    }

    public static CatalogHadoopContext create(Options options) {
        return new CatalogHadoopContext(options, null, null, null);
    }

    public static CatalogHadoopContext create(Options options, Configuration hadoopConf) {
        return new CatalogHadoopContext(options, hadoopConf, null, null);
    }

    public static CatalogHadoopContext create(Options options, FileIOLoader fallbackIOLoader) {
        return new CatalogHadoopContext(options, null, null, fallbackIOLoader);
    }

    public static CatalogHadoopContext create(
            Options options, FileIOLoader preferIOLoader, FileIOLoader fallbackIOLoader) {
        return new CatalogHadoopContext(options, null, preferIOLoader, fallbackIOLoader);
    }

    public static CatalogHadoopContext create(
            Options options,
            Configuration hadoopConf,
            FileIOLoader preferIOLoader,
            FileIOLoader fallbackIOLoader) {
        return new CatalogHadoopContext(options, hadoopConf, preferIOLoader, fallbackIOLoader);
    }

    /** Return hadoop {@link Configuration}. */
    public Configuration hadoopConf() {
        return hadoopConf.get();
    }
}
