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

package org.apache.paimon.vfs.hadoop;

import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/** Paimon virtual file system configuration. */
public class PaimonVirtualFileSystemConfiguration {
    public static final String PAIMON_VFS_PREFIX = "fs.pvfs.";

    public static Options convertToCatalogOptions(Configuration conf) {
        Options options = new Options();
        for (Map.Entry<String, String> entry : conf) {
            if (entry.getKey().startsWith(PAIMON_VFS_PREFIX)) {
                String key = entry.getKey().substring(PAIMON_VFS_PREFIX.length());
                options.set(key, entry.getValue());
            }
        }
        return options;
    }
}
