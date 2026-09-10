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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.utils.PartitionPathUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Resolves and validates catalog-managed Format Table partition locations. */
public final class FormatTablePartitionPathResolver {

    private final Path tablePath;
    private final String tableName;
    private final boolean onlyValueInPath;
    @Nullable private final CatalogContext catalogContext;
    private final Map<Map<String, String>, String> pathsBySpec = new LinkedHashMap<>();
    private final Map<String, OwnershipNode> ownershipRoots = new HashMap<>();

    FormatTablePartitionPathResolver(Path tablePath, String tableName, boolean onlyValueInPath) {
        this(tablePath, tableName, onlyValueInPath, null);
    }

    FormatTablePartitionPathResolver(
            Path tablePath,
            String tableName,
            boolean onlyValueInPath,
            @Nullable CatalogContext catalogContext) {
        this.tablePath = tablePath;
        this.tableName = tableName;
        this.onlyValueInPath = onlyValueInPath;
        this.catalogContext = catalogContext;
    }

    @Nullable
    static String customLocation(Partition partition) {
        Map<String, String> options = partition.options();
        if (options == null || !options.containsKey(CoreOptions.PATH.key())) {
            return null;
        }
        String location = options.get(CoreOptions.PATH.key());
        if (location == null) {
            throw new IllegalStateException("Partition path option must not be null.");
        }
        return location;
    }

    Path resolve(LinkedHashMap<String, String> spec, @Nullable String customLocation) {
        Path defaultPath =
                new Path(
                        tablePath,
                        PartitionPathUtils.generatePartitionPathUtil(spec, onlyValueInPath));
        if (customLocation == null) {
            return defaultPath;
        }

        try {
            return resolveCustomLocation(
                    tablePath, spec, onlyValueInPath, customLocation, catalogContext);
        } catch (IllegalArgumentException e) {
            throw invalidLocation(spec, e);
        }
    }

    /** Resolves a custom location using the catalog's Hadoop filesystem identity. */
    public static Path resolveCustomLocation(
            Path tablePath,
            LinkedHashMap<String, String> spec,
            boolean onlyValueInPath,
            String customLocation,
            @Nullable CatalogContext catalogContext) {
        PartitionPathUtils.validatePartitionSpecForPath(spec, onlyValueInPath);
        Path customPath = canonicalizeCustomLocation(customLocation, catalogContext);
        if (usesViewFileSystem(tablePath) || usesViewFileSystem(customPath)) {
            throw new IllegalArgumentException(
                    "Custom ViewFS partition locations require mount-table identity resolution.");
        }
        if (overlaps(customPath, tablePath, catalogContext)) {
            throw new IllegalArgumentException("Custom partition location overlaps table data.");
        }
        return customPath;
    }

    /**
     * Records a resolved path. Returns false for a repeated identical spec and path; callers skip
     * that entry so duplicate catalog rows do not produce duplicate data.
     */
    boolean validateAndRecord(LinkedHashMap<String, String> spec, Path path) {
        ResolvedPath resolved = ResolvedPath.of(path, catalogContext);
        String previousForSpec = pathsBySpec.get(spec);
        if (previousForSpec != null) {
            if (previousForSpec.equals(path.toString())) {
                return false;
            }
            throw overlappingLocations();
        }

        if (overlapsOwnedPath(resolved)) {
            throw overlappingLocations();
        }
        pathsBySpec.put(new LinkedHashMap<>(spec), path.toString());
        return true;
    }

    private boolean overlapsOwnedPath(ResolvedPath path) {
        OwnershipNode node =
                ownershipRoots.computeIfAbsent(path.fileSystem, ignored -> new OwnershipNode());
        String[] segments = path.pathSegments();
        for (String segment : segments) {
            // A terminal node reached before the candidate ends is an existing ancestor.
            if (node.owned) {
                return true;
            }
            node = node.children.computeIfAbsent(segment, ignored -> new OwnershipNode());
        }
        // A terminal final node is equality. Children below it make the candidate an ancestor.
        if (node.owned || !node.children.isEmpty()) {
            return true;
        }
        node.owned = true;
        return false;
    }

    /** Canonicalizes a custom location using the catalog's Hadoop configuration when present. */
    public static Path canonicalizeCustomLocation(
            String location, @Nullable CatalogContext catalogContext) {
        try {
            validateDecodedLocation(location);
            String decoded = decodePercentOnce(location);
            if (decoded.contains("%")) {
                throw new IllegalArgumentException("Invalid custom partition location.");
            }
            validateDecodedLocation(decoded);

            Path path = new Path(decoded);
            URI uri = path.toUri();
            String scheme = uri.getScheme();
            String authority = uri.getAuthority();
            String uriPath = uri.getPath();
            if (scheme == null
                    || scheme.isEmpty()
                    || (uri.getUserInfo() != null && !isAbfsAuthority(uri))
                    || uriPath == null
                    || !uriPath.startsWith(Path.SEPARATOR)
                    || uriPath.equals(Path.SEPARATOR)) {
                throw new IllegalArgumentException("Invalid custom partition location.");
            }

            scheme = scheme.toLowerCase(Locale.ROOT);
            if ((scheme.equals("file") && authority != null && !authority.isEmpty())
                    || (!scheme.equals("file")
                            && !scheme.equals("hdfs")
                            && (authority == null || authority.isEmpty()))) {
                throw new IllegalArgumentException("Invalid custom partition location.");
            }
            authority =
                    authority == null || authority.isEmpty()
                            ? null
                            : authority.toLowerCase(Locale.ROOT);
            Path canonical = new Path(scheme, authority, uriPath);
            return scheme.equals("hdfs")
                    ? canonicalizeHdfsPath(canonical, catalogContext)
                    : canonical;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid custom partition location.", e);
        }
    }

    private static boolean isAbfsAuthority(URI uri) {
        String scheme = uri.getScheme();
        String userInfo = uri.getUserInfo();
        return scheme != null
                && (scheme.equalsIgnoreCase("abfs") || scheme.equalsIgnoreCase("abfss"))
                && userInfo != null
                && !userInfo.isEmpty()
                && userInfo.indexOf(':') < 0
                && uri.getHost() != null;
    }

    private static boolean usesViewFileSystem(Path path) {
        String scheme = path.toUri().getScheme();
        return scheme != null && scheme.equalsIgnoreCase("viewfs");
    }

    private static Path canonicalizeHdfsPath(Path path, @Nullable CatalogContext catalogContext) {
        URI canonicalUri = canonicalHdfsUri(path.toUri(), catalogContext);
        if (canonicalUri.getAuthority() == null) {
            throw new IllegalArgumentException(
                    "Authorityless HDFS location requires an HDFS default filesystem.");
        }
        return new Path("hdfs", canonicalUri.getAuthority(), path.toUri().getPath());
    }

    private static URI canonicalHdfsUri(URI uri, @Nullable CatalogContext catalogContext) {
        URI resolved = uri;
        if (resolved.getAuthority() == null && catalogContext != null) {
            URI defaultUri = FileSystem.getDefaultUri(catalogContext.hadoopConf());
            if ("hdfs".equalsIgnoreCase(defaultUri.getScheme())
                    && defaultUri.getAuthority() != null) {
                resolved = defaultUri;
            }
        }
        if (resolved.getAuthority() == null) {
            return resolved;
        }
        String logicalNameservice = logicalHdfsNameservice(resolved, catalogContext);
        if (logicalNameservice != null) {
            return new Path("hdfs", logicalNameservice, "/").toUri();
        }
        URI physical = NetUtils.getCanonicalUri(resolved, 8020);
        return new Path("hdfs", physical.getAuthority().toLowerCase(Locale.ROOT), Path.SEPARATOR)
                .toUri();
    }

    @Nullable
    private static String logicalHdfsNameservice(URI uri, @Nullable CatalogContext catalogContext) {
        if (catalogContext == null || uri.getHost() == null) {
            return null;
        }
        String nameservices = catalogContext.hadoopConf().get("dfs.nameservices", "");
        String requestedAuthority = canonicalHdfsAuthority(uri);
        String match = null;
        for (String name : nameservices.split(",")) {
            String nameservice = name.trim();
            if (nameservice.isEmpty()) {
                continue;
            }
            boolean matches =
                    nameservice.equalsIgnoreCase(uri.getHost())
                            || matchesConfiguredHdfsAddress(
                                    requestedAuthority,
                                    catalogContext
                                            .hadoopConf()
                                            .get("dfs.namenode.rpc-address." + nameservice));
            String namenodes =
                    catalogContext.hadoopConf().get("dfs.ha.namenodes." + nameservice, "");
            for (String node : namenodes.split(",")) {
                String nodeId = node.trim();
                if (nodeId.isEmpty()) {
                    continue;
                }
                String address =
                        catalogContext
                                .hadoopConf()
                                .get("dfs.namenode.rpc-address." + nameservice + "." + nodeId);
                if (address == null || address.trim().isEmpty()) {
                    continue;
                }
                if (matchesConfiguredHdfsAddress(requestedAuthority, address)) {
                    matches = true;
                }
            }
            if (matches) {
                if (match != null && !match.equals(nameservice)) {
                    throw new IllegalArgumentException(
                            "HDFS authority belongs to multiple logical nameservices.");
                }
                match = nameservice;
            }
        }
        return match;
    }

    private static boolean matchesConfiguredHdfsAddress(
            String requestedAuthority, @Nullable String address) {
        if (address == null || address.trim().isEmpty()) {
            return false;
        }
        URI member = URI.create("hdfs://" + address.trim());
        return requestedAuthority.equals(canonicalHdfsAuthority(member));
    }

    private static String canonicalHdfsAuthority(URI uri) {
        return NetUtils.getCanonicalUri(uri, 8020).getAuthority().toLowerCase(Locale.ROOT);
    }

    private static void validateDecodedLocation(String location) {
        if (location == null
                || location.isEmpty()
                || isBoundaryWhitespace(location)
                || location.contains("?")
                || location.contains("#")
                || location.contains("\\")) {
            throw new IllegalArgumentException("Invalid custom partition location.");
        }

        for (int offset = 0; offset < location.length(); ) {
            int codePoint = location.codePointAt(offset);
            if (Character.isISOControl(codePoint)) {
                throw new IllegalArgumentException("Invalid custom partition location.");
            }
            offset += Character.charCount(codePoint);
        }

        for (String segment : location.split(Path.SEPARATOR, -1)) {
            if (segment.equals(Path.CUR_DIR) || segment.equals("..")) {
                throw new IllegalArgumentException("Invalid custom partition location.");
            }
        }
    }

    private static boolean isBoundaryWhitespace(String value) {
        int first = value.codePointAt(0);
        int last = value.codePointBefore(value.length());
        return isWhitespace(first) || isWhitespace(last);
    }

    private static boolean isWhitespace(int codePoint) {
        return Character.isWhitespace(codePoint) || Character.isSpaceChar(codePoint);
    }

    private static String decodePercentOnce(String value) {
        StringBuilder decoded = new StringBuilder(value.length());
        for (int offset = 0; offset < value.length(); ) {
            char current = value.charAt(offset);
            if (current != '%') {
                decoded.append(current);
                offset++;
                continue;
            }

            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            while (offset < value.length() && value.charAt(offset) == '%') {
                if (offset + 2 >= value.length()) {
                    throw new IllegalArgumentException("Invalid percent encoding in location.");
                }
                int high = Character.digit(value.charAt(offset + 1), 16);
                int low = Character.digit(value.charAt(offset + 2), 16);
                if (high < 0 || low < 0) {
                    throw new IllegalArgumentException("Invalid percent encoding in location.");
                }
                bytes.write((high << 4) + low);
                offset += 3;
            }
            try {
                decoded.append(
                        StandardCharsets.UTF_8
                                .newDecoder()
                                .onMalformedInput(CodingErrorAction.REPORT)
                                .onUnmappableCharacter(CodingErrorAction.REPORT)
                                .decode(ByteBuffer.wrap(bytes.toByteArray())));
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException("Invalid percent encoding in location.", e);
            }
        }
        return decoded.toString();
    }

    static boolean isWithin(Path candidate, Path root) {
        return isWithin(candidate, root, null);
    }

    static boolean isWithin(Path candidate, Path root, @Nullable CatalogContext catalogContext) {
        ResolvedPath candidatePath = ResolvedPath.of(candidate, catalogContext);
        ResolvedPath rootPath = ResolvedPath.of(root, catalogContext);
        return rootPath.equals(candidatePath) || rootPath.isAncestorOf(candidatePath);
    }

    private static boolean overlaps(
            Path left, Path right, @Nullable CatalogContext catalogContext) {
        ResolvedPath resolvedLeft = ResolvedPath.of(left, catalogContext);
        ResolvedPath resolvedRight = ResolvedPath.of(right, catalogContext);
        return resolvedLeft.equals(resolvedRight)
                || resolvedLeft.isAncestorOf(resolvedRight)
                || resolvedRight.isAncestorOf(resolvedLeft);
    }

    private IllegalStateException invalidLocation(
            Map<String, String> spec, IllegalArgumentException cause) {
        return new IllegalStateException(
                String.format(
                        "Catalog returned an invalid custom location for partition %s of Format Table %s.",
                        spec, tableName),
                cause);
    }

    private IllegalStateException overlappingLocations() {
        return new IllegalStateException(
                String.format(
                        "Catalog returned overlapping locations for different partitions of Format Table %s.",
                        tableName));
    }

    /**
     * One trie is maintained per filesystem. Visiting each path segment once is sufficient:
     * ancestors are terminal nodes on the route, equality is the terminal node at the route's end,
     * and descendants are children below that node.
     */
    private static final class OwnershipNode {

        private final Map<String, OwnershipNode> children = new HashMap<>();
        private boolean owned;
    }

    private static final class ResolvedPath {

        private final String fileSystem;
        private final String path;

        private ResolvedPath(String fileSystem, String path) {
            this.fileSystem = fileSystem;
            this.path = path;
        }

        private static ResolvedPath of(Path path, @Nullable CatalogContext catalogContext) {
            URI uri = path.toUri().normalize();
            String scheme = canonicalFileSystemScheme(uri.getScheme());
            URI fileSystemUri = scheme.equals("hdfs") ? canonicalHdfsUri(uri, catalogContext) : uri;
            String authority = fileSystemUri.getAuthority();
            authority = authority == null ? "" : authority.toLowerCase(Locale.ROOT);
            String normalizedPath = trimTrailingSeparators(uri.getPath());
            return new ResolvedPath(scheme + "://" + authority, normalizedPath);
        }

        private static String canonicalFileSystemScheme(@Nullable String scheme) {
            // An absolute path without a scheme and file:/ name the same local filesystem.
            if (scheme == null) {
                return "file";
            }
            String normalized = scheme.toLowerCase(Locale.ROOT);
            // These aliases address the same storage namespaces with different clients or
            // transport settings and therefore cannot establish separate ownership boundaries.
            if (normalized.equals("abfss")) {
                return "abfs";
            }
            if (normalized.equals("s3a") || normalized.equals("s3n")) {
                return "s3";
            }
            return normalized;
        }

        private boolean isAncestorOf(ResolvedPath other) {
            if (!fileSystem.equals(other.fileSystem) || path.equals(other.path)) {
                return false;
            }
            if (path.equals(Path.SEPARATOR)) {
                return other.path.startsWith(Path.SEPARATOR);
            }
            return other.path.startsWith(path + Path.SEPARATOR);
        }

        private String[] pathSegments() {
            return path.substring(1).split(Path.SEPARATOR, -1);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ResolvedPath that = (ResolvedPath) o;
            return fileSystem.equals(that.fileSystem) && path.equals(that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileSystem, path);
        }

        private static String trimTrailingSeparators(String path) {
            int end = path.length();
            while (end > 1 && path.charAt(end - 1) == Path.SEPARATOR_CHAR) {
                end--;
            }
            return path.substring(0, end);
        }
    }
}
