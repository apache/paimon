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

package org.apache.paimon.jindo;

import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.FileType;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.paimon.CoreOptions.CHANGELOG_FILE_PREFIX;
import static org.apache.paimon.CoreOptions.DATA_FILE_PREFIX;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_OSS_ENDPOINT;

/**
 * Cache endpoints that reads of immutable Paimon files may use instead of the OSS endpoint, chosen
 * by file type from {@code io-cache.targets} (or {@code io-cache.endpoint}), {@code
 * io-cache.routes} and {@code io-cache.whitelist}.
 */
final class CacheEndpoints implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Pattern ENDPOINT_NAME = Pattern.compile("[a-z][a-z0-9-]*");

    @Nullable private final String ossEndpoint;
    private final List<CacheEndpoint> endpoints;
    private final Map<FileType, CacheEndpoint> endpointByType;
    private final List<String> dataFilePrefixes;

    private CacheEndpoints(
            @Nullable String ossEndpoint,
            List<CacheEndpoint> endpoints,
            Map<FileType, CacheEndpoint> endpointByType,
            List<String> dataFilePrefixes) {
        this.ossEndpoint = ossEndpoint;
        this.endpoints = endpoints;
        this.endpointByType = endpointByType;
        this.dataFilePrefixes = dataFilePrefixes;
    }

    /** The cache endpoints of the options, or null when they name none. */
    @Nullable
    static CacheEndpoints create(Options options) {
        // a client-side dlf.oss-endpoint replaces every endpoint the token vends
        if (!StringUtils.isNullOrWhitespaceOnly(options.get(DLF_OSS_ENDPOINT.key()))) {
            return null;
        }
        Map<String, String> urls = endpointUrls(options);
        if (urls.isEmpty()) {
            return null;
        }
        Map<String, CacheEndpoint> byName = new LinkedHashMap<>();
        urls.forEach(
                (name, url) -> {
                    if (!StringUtils.isNullOrWhitespaceOnly(url)) {
                        byName.put(name, endpoint(options, name, url.trim()));
                    }
                });

        String whitelist = options.get("io-cache.whitelist");
        Set<FileType> types =
                whitelist == null ? EnumSet.allOf(FileType.class) : fileTypes(whitelist);
        String routes = options.get("io-cache.routes");
        Map<FileType, CacheEndpoint> endpointByType = new EnumMap<>(FileType.class);
        for (FileType type : types) {
            CacheEndpoint endpoint =
                    routes == null
                            ? byName.values().stream().findFirst().orElse(null)
                            : byName.get(routedName(routes, type));
            if (endpoint != null) {
                endpointByType.put(type, endpoint);
            }
        }

        String origin = options.get("io-cache.origin.endpoint");
        return new CacheEndpoints(
                StringUtils.isNullOrWhitespaceOnly(origin)
                        ? options.get("fs.oss.endpoint")
                        : origin,
                new ArrayList<>(byName.values()),
                endpointByType,
                dataFilePrefixes(options));
    }

    /** The OSS endpoint, which every request that does not use a cache endpoint goes to. */
    @Nullable
    String ossEndpoint() {
        return ossEndpoint;
    }

    Collection<CacheEndpoint> endpoints() {
        return endpoints;
    }

    /** The cache endpoint of a file, or null when it must be read from the OSS endpoint. */
    @Nullable
    CacheEndpoint endpointOf(Path path) {
        FileType type = cacheableType(path, dataFilePrefixes);
        return type == null ? null : endpointByType.get(type);
    }

    /**
     * The type of a file a cache may serve; null if it is rewritten in place, probed or unknown.
     */
    @Nullable
    static FileType cacheableType(Path path, List<String> dataFilePrefixes) {
        String name = path.getName();
        if (FileType.isMutable(path) || isProbedBeforeWritten(path)) {
            return null;
        }
        FileType type = FileType.classify(path);
        return type != FileType.DATA || dataFilePrefixes.stream().anyMatch(name::startsWith)
                ? type
                : null;
    }

    // snapshot-N, schema-N and changelog/changelog-N: readers look for the next id before it exists
    private static boolean isProbedBeforeWritten(Path path) {
        String name = path.getName();
        Path parent = path.getParent();
        return name.startsWith("snapshot-")
                || name.startsWith("schema-")
                || (name.startsWith("changelog-")
                        && parent != null
                        && "changelog".equals(parent.getName()));
    }

    static List<String> dataFilePrefixes(Options options) {
        List<String> prefixes = new ArrayList<>();
        prefixes.add(DATA_FILE_PREFIX.defaultValue());
        prefixes.add(CHANGELOG_FILE_PREFIX.defaultValue());
        for (String key : new String[] {DATA_FILE_PREFIX.key(), CHANGELOG_FILE_PREFIX.key()}) {
            String prefix = options.get(key);
            if (!StringUtils.isNullOrWhitespaceOnly(prefix) && !prefixes.contains(prefix)) {
                prefixes.add(prefix);
            }
        }
        return prefixes;
    }

    // io-cache.targets with io-cache.target.<name>.endpoint, else io-cache.endpoint as "default"
    private static Map<String, String> endpointUrls(Options options) {
        Map<String, String> urls = new LinkedHashMap<>();
        String names = options.get("io-cache.targets");
        if (names == null) {
            String url = options.get("io-cache.endpoint");
            if (url != null) {
                urls.put("default", url);
            }
            return urls;
        }
        for (String name : names.toLowerCase(Locale.ROOT).split(",")) {
            name = name.trim();
            if (ENDPOINT_NAME.matcher(name).matches() && !urls.containsKey(name)) {
                urls.put(name, options.get("io-cache.target." + name + ".endpoint"));
            }
        }
        return urls;
    }

    private static CacheEndpoint endpoint(Options options, String name, String url) {
        String prefix = "io-cache.target." + name + ".";
        String region = options.get(prefix + "region");
        String pathStyle = options.get(prefix + "path-style-access");
        return new CacheEndpoint(
                name,
                url.contains("://") ? url : "https://" + url,
                pathStyle != null && "true".equalsIgnoreCase(pathStyle.trim()),
                StringUtils.isNullOrWhitespaceOnly(region) ? null : region);
    }

    // io-cache.routes is "types=name;...": the first rule listing the type names its endpoint
    @Nullable
    static String routedName(String routes, FileType type) {
        for (String rule : routes.toLowerCase(Locale.ROOT).split(";")) {
            int eq = rule.indexOf('=');
            String name = eq > 0 ? rule.substring(eq + 1).trim() : "";
            if (!name.isEmpty() && fileTypes(rule.substring(0, eq)).contains(type)) {
                return name;
            }
        }
        return null;
    }

    private static Set<FileType> fileTypes(String value) {
        return FileType.parseWhitelist(value.toLowerCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return "CacheEndpoints{" + endpointByType + ", oss=" + ossEndpoint + "}";
    }

    /** A cache endpoint with its addressing settings. */
    static final class CacheEndpoint implements Serializable {

        private static final long serialVersionUID = 1L;

        final String name;
        final String url;
        final boolean pathStyleAccess;
        @Nullable final String region;

        CacheEndpoint(String name, String url, boolean pathStyleAccess, @Nullable String region) {
            this.name = name;
            this.url = url;
            this.pathStyleAccess = pathStyleAccess;
            this.region = region;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CacheEndpoint)) {
                return false;
            }
            CacheEndpoint that = (CacheEndpoint) o;
            return name.equals(that.name)
                    && url.equals(that.url)
                    && pathStyleAccess == that.pathStyleAccess
                    && Objects.equals(region, that.region);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, url, pathStyleAccess, region);
        }

        @Override
        public String toString() {
            return name + "=" + url;
        }
    }
}
