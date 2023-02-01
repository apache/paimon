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

package org.apache.flink.table.store.oss;

import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.options.CatalogOptions;
import org.apache.flink.table.store.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/** OSS {@link FileIO}. */
public class OSSFileIO extends HadoopCompliantFileIO {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(OSSFileIO.class);

    /**
     * In order to simplify, we make flink oss configuration keys same with hadoop oss module. So,
     * we add all configuration key with prefix `fs.oss` in flink conf to hadoop conf.
     */
    private static final String[] CONFIG_PREFIXES = {"fs.oss."};

    private static final String OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId";
    private static final String OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    private static final String OSS_SECURITY_TOKEN = "fs.oss.securityToken";

    private static final Map<String, String> CASE_SENSITIVE_KEYS =
            new HashMap<String, String>() {
                {
                    put(OSS_ACCESS_KEY_ID.toLowerCase(), OSS_ACCESS_KEY_ID);
                    put(OSS_ACCESS_KEY_SECRET.toLowerCase(), OSS_ACCESS_KEY_SECRET);
                    put(OSS_SECURITY_TOKEN.toLowerCase(), OSS_SECURITY_TOKEN);
                }
            };

    private Options hadoopOptions;

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public void configure(CatalogOptions config) {
        hadoopOptions = new Options();
        // read all configuration with prefix 'CONFIG_PREFIXES'
        for (String key : config.keySet()) {
            for (String prefix : CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String value = config.get(key);
                    if (CASE_SENSITIVE_KEYS.containsKey(key.toLowerCase())) {
                        key = CASE_SENSITIVE_KEYS.get(key.toLowerCase());
                    }
                    hadoopOptions.set(key, value);

                    LOG.debug(
                            "Adding config entry for {} as {} to Hadoop config",
                            key,
                            hadoopOptions.get(key));
                }
            }
        }
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

        AliyunOSSFileSystem fs = new AliyunOSSFileSystem();
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
