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

package org.apache.paimon.cache;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.BlockCacheType;
import org.apache.paimon.options.MemorySize;

import java.io.File;

/** The config for block cache. */
public class BlockCacheConfig {

    public static final BlockCacheConfig DISABLED = new BlockCacheConfig(null, null, null, false);

    public final MemorySize disk;

    public final File localPath;

    public final BlockCacheType blockCacheType;

    public final boolean enabled;

    private BlockCacheConfig(
            MemorySize disk,
            File localPath,
            BlockCacheType blockCacheType,
            boolean blockCacheEnabled) {
        this.disk = disk;
        this.localPath = localPath;
        this.blockCacheType = blockCacheType;
        this.enabled = blockCacheEnabled;
    }

    public BlockCacheConfig(CoreOptions options) {
        this(
                options.diskSize(),
                new File(options.diskPath()),
                options.blockCacheType(),
                options.blockCacheEnabled());
    }

    @Override
    public String toString() {
        return "BlockCacheConfig{"
                + "disk="
                + disk
                + ", localPath="
                + localPath
                + ", blockCacheType="
                + blockCacheType
                + ", enabled="
                + enabled
                + '}';
    }
}
