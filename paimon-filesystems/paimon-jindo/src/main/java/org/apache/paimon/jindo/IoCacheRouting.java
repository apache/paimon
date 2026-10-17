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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.CHANGELOG_FILE_PREFIX;
import static org.apache.paimon.CoreOptions.DATA_FILE_PREFIX;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_OSS_ENDPOINT;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ENABLED;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_POLICY;

/**
 * Selects cache targets for immutable Paimon files using {@code io-cache.targets}, {@code
 * io-cache.routes} and {@code io-cache.whitelist}. The endpoint mode also carries the normalized
 * {@code io-cache.policy} read, metadata, existence and write switches.
 */
final class IoCacheRouting implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String META_CACHE_ENABLED_TAG = "meta";
    private static final String READ_CACHE_ENABLED_TAG = "read";
    private static final String WRITE_CACHE_ENABLED_TAG = "write";
    // exists asks whether a file is still there, which a cache of deleted files can get wrong
    private static final String EXISTS_CACHE_ENABLED_TAG = "exists";
    private static final String DISABLE_CACHE_TAG = "none";

    private static final Pattern TARGET_NAME = Pattern.compile("[a-z][a-z0-9-]*");

    // Data files are named {prefix}{uuid}-{count}.{extension}; this matches what follows the
    // prefix.
    private static final String UUID =
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
    private static final Pattern DATA_FILE_SUFFIX = Pattern.compile(UUID + "-[0-9]+\\..+");

    // Other types are routed only under the names Paimon writes (paimon-rust adds -changelog to
    // changelog manifests); Format Table files may be replaced in place.
    private static final Pattern META_FILE =
            Pattern.compile(
                    "(manifest-list|index-manifest|stat)-"
                            + UUID
                            + "-[0-9]+|manifest-"
                            + UUID
                            + "(-changelog)?-[0-9]+(\\.avro\\.sidecar)?");
    private static final Pattern BUCKET_INDEX_FILE = Pattern.compile("index-" + UUID + "-[0-9]+");
    private static final Pattern GLOBAL_INDEX_FILE =
            Pattern.compile("[a-z0-9_-]+-global-index-" + UUID + "\\.index");

    private static final String MANIFEST_SIDECAR_SUFFIX = ".avro.sidecar";

    @Nullable private final String ossEndpoint;
    private final List<CacheTarget> targets;
    private final Map<FileType, CacheTarget> targetByType;
    private final List<String> dataFilePrefixes;
    private final boolean metaCacheEnabled;
    private final boolean readCacheEnabled;
    private final boolean writeCacheEnabled;
    private final boolean existsCacheEnabled;

    private IoCacheRouting(
            @Nullable String ossEndpoint,
            List<CacheTarget> targets,
            Map<FileType, CacheTarget> targetByType,
            List<String> dataFilePrefixes,
            boolean metaCacheEnabled,
            boolean readCacheEnabled,
            boolean writeCacheEnabled,
            boolean existsCacheEnabled) {
        this.ossEndpoint = ossEndpoint;
        this.targets = targets;
        this.targetByType = targetByType;
        this.dataFilePrefixes = dataFilePrefixes;
        this.metaCacheEnabled = metaCacheEnabled;
        this.readCacheEnabled = readCacheEnabled;
        this.writeCacheEnabled = writeCacheEnabled;
        this.existsCacheEnabled = existsCacheEnabled;
    }

    /** Creates endpoint routing from the options, or null when routing is not enabled. */
    @Nullable
    static IoCacheRouting create(Options options) {
        if (!options.get(IO_CACHE_ENABLED)) {
            return null;
        }
        Set<String> policy = parsePolicy(options.get(IO_CACHE_POLICY));
        boolean metaCache = policy.contains(META_CACHE_ENABLED_TAG);
        boolean readCache = policy.contains(READ_CACHE_ENABLED_TAG);
        boolean writeCache = policy.contains(WRITE_CACHE_ENABLED_TAG);
        boolean existsCache = policy.contains(EXISTS_CACHE_ENABLED_TAG);
        if (policy.contains(DISABLE_CACHE_TAG)
                || !(metaCache || readCache || writeCache || existsCache)) {
            return null;
        }
        // a client-side dlf.oss-endpoint replaces every endpoint the token vends
        if (!StringUtils.isNullOrWhitespaceOnly(options.get(DLF_OSS_ENDPOINT.key()))) {
            return null;
        }
        Map<String, String> urls = declaredTargets(options);
        if (urls.isEmpty()) {
            return null;
        }
        Map<String, CacheTarget> byName = new LinkedHashMap<>();
        urls.forEach(
                (name, url) -> {
                    if (!StringUtils.isNullOrWhitespaceOnly(url)) {
                        byName.put(name, target(options, name, url.trim()));
                    }
                });

        String whitelist = options.get("io-cache.whitelist");
        Set<FileType> types =
                whitelist == null ? EnumSet.allOf(FileType.class) : fileTypes(whitelist);
        String routes = options.get("io-cache.routes");
        Map<FileType, CacheTarget> targetByType = new EnumMap<>(FileType.class);
        for (FileType type : types) {
            CacheTarget target =
                    routes == null
                            ? byName.values().stream().findFirst().orElse(null)
                            : byName.get(routedName(routes, type));
            if (target != null) {
                targetByType.put(type, target);
            }
        }

        String origin = options.get("io-cache.origin.endpoint");
        return new IoCacheRouting(
                StringUtils.isNullOrWhitespaceOnly(origin)
                        ? options.get("fs.oss.endpoint")
                        : origin,
                new ArrayList<>(byName.values()),
                targetByType,
                dataFilePrefixes(options),
                metaCache,
                readCache,
                writeCache,
                existsCache);
    }

    private static Set<String> parsePolicy(@Nullable String value) {
        return value == null
                ? Collections.emptySet()
                : Arrays.stream(value.split(","))
                        .map(token -> token.trim().toLowerCase(Locale.ROOT))
                        .collect(Collectors.toSet());
    }

    boolean metaCacheEnabled() {
        return metaCacheEnabled;
    }

    boolean readCacheEnabled() {
        return readCacheEnabled;
    }

    boolean writeCacheEnabled() {
        return writeCacheEnabled;
    }

    boolean existsCacheEnabled() {
        return existsCacheEnabled;
    }

    /** The OSS endpoint used by requests that do not use a cache target. */
    @Nullable
    String ossEndpoint() {
        return ossEndpoint;
    }

    Collection<CacheTarget> targets() {
        return targets;
    }

    /** The cache target selected for a file, or null when it does not use a cache target. */
    @Nullable
    CacheTarget targetOf(Path path) {
        // Only OSS paths can use cache targets.
        if (!"oss".equals(path.toUri().getScheme())) {
            return null;
        }
        FileType type = cacheableType(path, dataFilePrefixes);
        return type == null ? null : targetByType.get(type);
    }

    /** The type of a file a cache may serve, or null when it must be read from OSS. */
    @Nullable
    static FileType cacheableType(Path path, List<String> dataFilePrefixes) {
        String name = path.getName();
        if (isTemporary(name) || FileType.isMutable(path)) {
            return null;
        }
        if (isDataFileName(name, dataFilePrefixes)) {
            return name.endsWith(".index") ? FileType.FILE_INDEX : FileType.DATA;
        }
        if (isSequential(path)) {
            return null;
        }
        FileType type = FileType.classify(path);
        return isPaimonFileName(type, name) ? type : null;
    }

    private static boolean isPaimonFileName(FileType type, String name) {
        switch (type) {
            case META:
                return META_FILE.matcher(name).matches();
            case BUCKET_INDEX:
                return BUCKET_INDEX_FILE.matcher(name).matches();
            case GLOBAL_INDEX:
                return GLOBAL_INDEX_FILE.matcher(name).matches();
            default:
                // data files and their file indexes are recognized by isDataFileName
                return false;
        }
    }

    // Manifests, indexes and statistics share the uuid-count shape but have no extension.
    private static boolean isDataFileName(String name, List<String> dataFilePrefixes) {
        if (name.endsWith(MANIFEST_SIDECAR_SUFFIX)) {
            return false;
        }
        for (String prefix : dataFilePrefixes) {
            if (name.startsWith(prefix)
                    && DATA_FILE_SUFFIX.matcher(name.substring(prefix.length())).matches()) {
                return true;
            }
        }
        return false;
    }

    private static boolean isTemporary(String name) {
        return name.endsWith(".tmp") || name.contains(".tmp-") || name.contains(".tmp.");
    }

    // Sequential metadata can be read before it exists; a cached NotFound would hide new commits.
    private static boolean isSequential(Path path) {
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
    private static Map<String, String> declaredTargets(Options options) {
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
            if (TARGET_NAME.matcher(name).matches() && !urls.containsKey(name)) {
                urls.put(name, options.get("io-cache.target." + name + ".endpoint"));
            }
        }
        return urls;
    }

    private static CacheTarget target(Options options, String name, String url) {
        String prefix = "io-cache.target." + name + ".";
        String region = options.get(prefix + "region");
        String pathStyle = options.get(prefix + "path-style-access");
        return new CacheTarget(
                name,
                url.contains("://") ? url : "https://" + url,
                pathStyle != null && "true".equalsIgnoreCase(pathStyle.trim()),
                StringUtils.isNullOrWhitespaceOnly(region) ? null : region);
    }

    // io-cache.routes is "types=name;...": the first matching rule selects the target.
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
        return "IoCacheRouting{" + targetByType + ", oss=" + ossEndpoint + "}";
    }

    /** A named cache target with its endpoint and addressing settings. */
    static final class CacheTarget implements Serializable {

        private static final long serialVersionUID = 1L;

        final String name;
        final String url;
        final boolean pathStyleAccess;
        @Nullable final String region;

        CacheTarget(String name, String url, boolean pathStyleAccess, @Nullable String region) {
            this.name = name;
            this.url = url;
            this.pathStyleAccess = pathStyleAccess;
            this.region = region;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CacheTarget)) {
                return false;
            }
            CacheTarget that = (CacheTarget) o;
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
