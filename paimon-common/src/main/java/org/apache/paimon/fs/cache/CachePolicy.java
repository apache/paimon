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

package org.apache.paimon.fs.cache;

import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.FileType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_POLICY;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_WHITELIST;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_WHITELIST_PATH;

/** Backend-independent routing policy supplied by the REST server. */
public final class CachePolicy implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean enabled;
    private final boolean metaEnabled;
    private final boolean readEnabled;
    private final boolean writeEnabled;
    private final Set<FileType> whitelist;
    private final boolean allPaths;
    private final List<String> paths;

    public CachePolicy(Options options) {
        String policy = options.get(IO_CACHE_POLICY);
        enabled = policy != null && !policy.contains("none");
        metaEnabled = enabled && policy.contains("meta");
        readEnabled = enabled && policy.contains("read");
        writeEnabled = enabled && policy.contains("write");
        whitelist = FileType.parseWhitelist(options.get(IO_CACHE_WHITELIST));
        // Keep the option's original default for legacy JindoCache while remote endpoint
        // routing allows all paths by default.
        String whitelistPath =
                options.contains(IO_CACHE_WHITELIST_PATH)
                        ? options.get(IO_CACHE_WHITELIST_PATH)
                        : "*";
        allPaths = whitelistPath != null && "*".equals(whitelistPath.trim());
        paths = new ArrayList<>();
        if (whitelistPath != null && !allPaths) {
            for (String pattern : whitelistPath.split(",")) {
                pattern = pattern.trim();
                if (!pattern.isEmpty()) {
                    paths.add(pattern);
                }
            }
        }
    }

    public boolean enabled() {
        return enabled;
    }

    public boolean shouldCache(Path path, String operation) {
        boolean operationEnabled =
                (metaEnabled && "meta".equalsIgnoreCase(operation))
                        || (readEnabled && "read".equalsIgnoreCase(operation))
                        || (writeEnabled && "write".equalsIgnoreCase(operation));
        if (!operationEnabled
                || FileType.isMutable(path)
                || !whitelist.contains(FileType.classify(path))) {
            return false;
        }
        if (allPaths) {
            return true;
        }
        String uriPath = path.toUri().getPath();
        for (String pattern : paths) {
            if (uriPath.contains(pattern)) {
                return true;
            }
        }
        return false;
    }
}
