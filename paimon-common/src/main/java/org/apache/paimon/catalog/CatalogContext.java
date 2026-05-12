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

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.utils.HadoopUtils.HADOOP_LOAD_DEFAULT_CONFIG;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Context of catalog for Hadoop-free environments.
 *
 * <p>This class provides basic catalog context without Hadoop dependencies. The factory methods
 * ({@link #create(Options)}, etc.) automatically detect whether Hadoop Configuration is needed and
 * return the appropriate type ({@link CatalogContext} or {@link CatalogHadoopContext}).
 *
 * <h3>When CatalogContext is Used</h3>
 *
 * <p>The factory will create a basic {@code CatalogContext} when:
 *
 * <ul>
 *   <li>Working with local filesystem or cloud storage (S3, Azure, GCS) without Hadoop
 *   <li>No Hadoop-based features are required
 *   <li>Running in a non-Hadoop environment
 * </ul>
 *
 * <h3>When CatalogHadoopContext is Used</h3>
 *
 * <p>The factory automatically creates a {@link CatalogHadoopContext} when it detects:
 *
 * <ul>
 *   <li><b>Hive metastore</b>: {@code metastore=hive} option is set
 *   <li><b>HDFS filesystem</b>: Warehouse path starts with {@code hdfs://}, {@code viewfs://}, or
 *       {@code har://}
 *   <li><b>Kerberos security</b>: Any Kerberos-related options are configured
 *   <li><b>Hadoop environment</b>: {@code HADOOP_CONF_DIR} or {@code HADOOP_HOME} environment
 *       variables are set
 *   <li><b>Explicit option</b>: {@code hadoop-load-default-config=true} is set
 * </ul>
 *
 * <h3>Direct Type Selection</h3>
 *
 * <p>For explicit control, call {@link CatalogHadoopContext#create(Options)} directly instead of
 * using this factory.
 *
 * @since 0.4.0
 * @see CatalogHadoopContext
 * @see HadoopAware
 */
@Public
public class CatalogContext implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final Options options;
    @Nullable protected final FileIOLoader preferIOLoader;
    @Nullable protected final FileIOLoader fallbackIOLoader;

    protected CatalogContext(
            Options options,
            @Nullable FileIOLoader preferIOLoader,
            @Nullable FileIOLoader fallbackIOLoader) {
        this.options = checkNotNull(options);
        this.preferIOLoader = preferIOLoader;
        this.fallbackIOLoader = fallbackIOLoader;
    }

    /**
     * Creates a copy of this context with different options.
     *
     * @param options new options
     * @return a new context instance with the specified options
     */
    public CatalogContext copy(Options options) {
        return create(options, this.preferIOLoader, this.fallbackIOLoader);
    }

    public CatalogContext copy(
            Options options, FileIOLoader preferIOLoader, FileIOLoader fileIOLoader) {
        return create(options, preferIOLoader, fileIOLoader);
    }

    public static CatalogContext create(Path warehouse) {
        Options options = new Options();
        options.set(WAREHOUSE, warehouse.toString());
        return create(options);
    }

    public static CatalogContext create(Options options) {
        return create(options, null, null);
    }

    public static CatalogContext create(Options options, FileIOLoader fallbackIOLoader) {
        return create(options, null, fallbackIOLoader);
    }

    public static CatalogContext create(
            Options options, FileIOLoader preferIOLoader, FileIOLoader fallbackIOLoader) {
        return shouldUseHadoopContext(options)
                ? CatalogHadoopContext.create(options, preferIOLoader, fallbackIOLoader)
                : new CatalogContext(options, preferIOLoader, fallbackIOLoader);
    }

    /**
     * Determines whether to use {@link CatalogHadoopContext} based on multiple detection criteria.
     *
     * <p>This method intelligently detects whether Hadoop Configuration is needed by checking (in
     * order of priority):
     *
     * <ol>
     *   <li><b>Metastore type</b>: Hive metastore requires Hadoop Configuration
     *   <li><b>Filesystem type</b>: HDFS and Hadoop-compatible filesystems require Hadoop
     *       Configuration
     *   <li><b>Security configuration</b>: Kerberos authentication requires Hadoop Configuration
     *   <li><b>Explicit option</b>: The {@code hadoop-load-default-config} option
     * </ol>
     *
     * <p>Note: Environment variable detection (HADOOP_CONF_DIR/HADOOP_HOME) is not used as it may
     * give false positives in development/testing environments where Hadoop is present but not
     * required for the current catalog.
     *
     * @param options catalog options
     * @return true if {@link CatalogHadoopContext} should be used, false otherwise
     */
    private static boolean shouldUseHadoopContext(Options options) {
        // Check metastore type (Hive requires Hadoop)
        if (needsHadoopForMetastore(options)) {
            return true;
        }

        // Check filesystem type (HDFS and similar require Hadoop)
        if (needsHadoopForFilesystem(options)) {
            return true;
        }

        // Check security configuration (Kerberos requires Hadoop)
        if (needsHadoopForSecurity(options)) {
            return true;
        }

        // Fall back to explicit option
        return options.getBoolean(HADOOP_LOAD_DEFAULT_CONFIG.key(), false);
    }

    /**
     * Checks if Hadoop context is needed based on metastore configuration.
     *
     * <p>Certain metastores (like Hive) require Hadoop Configuration for HMS client creation and
     * table operations.
     *
     * @param options catalog options
     * @return true if Hadoop context is required for the configured metastore
     */
    private static boolean needsHadoopForMetastore(Options options) {
        if (!options.contains(METASTORE)) {
            return false;
        }
        String metastore = options.get(METASTORE);
        // Hive metastore requires Hadoop configuration
        return "hive".equalsIgnoreCase(metastore);
    }

    /**
     * Checks if Hadoop context is needed based on warehouse filesystem.
     *
     * <p>HDFS and certain Hadoop-compatible filesystems require Hadoop Configuration for file
     * operations.
     *
     * @param options catalog options
     * @return true if Hadoop context is required for the warehouse filesystem
     */
    private static boolean needsHadoopForFilesystem(Options options) {
        if (!options.contains(WAREHOUSE)) {
            return false;
        }
        String warehouse = options.get(WAREHOUSE);
        // HDFS and some Hadoop filesystems need Hadoop configuration
        return warehouse.startsWith("hdfs://")
                || warehouse.startsWith("viewfs://")
                || warehouse.startsWith("har://");
    }

    /**
     * Checks if Hadoop context is needed based on security configuration.
     *
     * <p>Kerberos authentication and other Hadoop security features require Hadoop Configuration.
     *
     * @param options catalog options
     * @return true if Hadoop context is required for security features
     */
    private static boolean needsHadoopForSecurity(Options options) {
        // Check for Kerberos configuration
        return options.containsKey("security.kerberos.login.principal")
                || options.containsKey("security.kerberos.login.keytab")
                || options.containsKey("security.kerberos.login.use-ticket-cache");
    }

    /**
     * Returns the catalog options.
     *
     * @return catalog options
     */
    public Options options() {
        return options;
    }

    /**
     * Returns the preferred file I/O loader, if configured.
     *
     * @return preferred file I/O loader, or null if not configured
     */
    @Nullable
    public FileIOLoader preferIO() {
        return preferIOLoader;
    }

    /**
     * Returns the fallback file I/O loader, if configured.
     *
     * @return fallback file I/O loader, or null if not configured
     */
    @Nullable
    public FileIOLoader fallbackIO() {
        return fallbackIOLoader;
    }
}
