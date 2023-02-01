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

import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.options.CatalogOptions;
import org.apache.flink.table.store.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/** S3 {@link FileIO}. */
public class S3FileIO extends HadoopCompliantFileIO {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(S3FileIO.class);

    private static final String[] CONFIG_PREFIXES = {"s3.", "s3a.", "fs.s3a."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.s3a.";

    private static final String[][] MIRRORED_CONFIG_KEYS = {
        {"fs.s3a.access-key", "fs.s3a.access.key"},
        {"fs.s3a.secret-key", "fs.s3a.secret.key"},
        {"fs.s3a.path-style-access", "fs.s3a.path.style.access"}
    };

    private Options hadoopOptions;

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public void configure(CatalogOptions config) {
        this.hadoopOptions = mirrorCertainHadoopConfig(loadHadoopConfigFromFlink(config));
    }

    // add additional config entries from the IO config to the Hadoop config
    private Options loadHadoopConfigFromFlink(CatalogOptions options) {
        Options hadoopConfig = new Options();
        for (String key : options.keySet()) {
            for (String prefix : CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String newKey = HADOOP_CONFIG_PREFIX + key.substring(prefix.length());
                    String value = options.get(key);
                    hadoopConfig.set(newKey, value);

                    LOG.debug("Adding config entry for {} as {} to Hadoop config", key, newKey);
                }
            }
        }
        return hadoopConfig;
    }

    // mirror certain keys to make use more uniform across implementations
    // with different keys
    private Options mirrorCertainHadoopConfig(Options hadoopConfig) {
        for (String[] mirrored : MIRRORED_CONFIG_KEYS) {
            String value = hadoopConfig.get(mirrored[0]);
            if (value != null) {
                hadoopConfig.set(mirrored[1], value);
            }
        }
        return hadoopConfig;
    }

    @Override
    protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) throws IOException {
        Configuration hadoopConf = new Configuration();
        hadoopOptions.toMap().forEach(hadoopConf::set);
        URI fsUri = path.toUri();
        final String scheme = fsUri.getScheme();
        final String authority = fsUri.getAuthority();

        if (scheme == null && authority == null) {
            fsUri = FileSystem.getDefaultUri(hadoopConf);
        } else if (scheme != null && authority == null) {
            URI defaultUri = FileSystem.getDefaultUri(hadoopConf);
            if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
                fsUri = defaultUri;
            }
        }

        S3AFileSystem fs = new S3AFileSystem();
        fs.initialize(fsUri, hadoopConf);
        return fs;
    }

    @Override
    public synchronized void close() throws IOException {
        if (fs != null) {
            fs.close();
            fs = null;
        }
    }
}
