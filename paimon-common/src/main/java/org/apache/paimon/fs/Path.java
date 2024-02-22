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

package org.apache.paimon.fs;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.StringUtils;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.regex.Pattern;

/* This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Names a file or directory in a {@link FileIO}. Path strings use slash as the directory separator.
 *
 * @since 0.4.0
 */
@Public
public class Path implements Comparable<Path>, Serializable {

    private static final long serialVersionUID = 1L;

    /** The directory separator, a slash. */
    public static final String SEPARATOR = "/";

    /** The directory separator, a slash, as a character. */
    public static final char SEPARATOR_CHAR = '/';

    /** The current directory, ".". */
    public static final String CUR_DIR = ".";

    /** Whether the current host is a Windows machine. */
    public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

    /** A pre-compiled regex/state-machine to match the path formats pattern. */
    private static final Pattern HAS_DRIVE_LETTER_SPECIFIER = Pattern.compile("^/?[a-zA-Z]:");

    /** A pre-compiled regex/state-machine to match the duplicated slashes pattern. */
    private static final Pattern SLASHES = Pattern.compile("/+");

    /** A hierarchical URI. */
    private URI uri;

    /**
     * Create a new Path based on the child path resolved against the parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(String parent, String child) {
        this(new Path(parent), new Path(child));
    }

    /**
     * Create a new Path based on the child path resolved against the parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(Path parent, String child) {
        this(parent, new Path(child));
    }

    /**
     * Create a new Path based on the child path resolved against the parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(String parent, Path child) {
        this(new Path(parent), child);
    }

    /**
     * Create a new Path based on the child path resolved against the parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(Path parent, Path child) {
        // Add a slash to parent's path so resolution is compatible with URI's
        URI parentUri = parent.uri;
        String parentPath = parentUri.getPath();
        if (!(parentPath.equals(SEPARATOR) || parentPath.isEmpty())) {
            try {
                parentUri =
                        new URI(
                                parentUri.getScheme(),
                                parentUri.getAuthority(),
                                parentUri.getPath() + SEPARATOR,
                                null,
                                parentUri.getFragment());
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }
        URI resolved = parentUri.resolve(child.uri);
        initialize(
                resolved.getScheme(),
                resolved.getAuthority(),
                resolved.getPath(),
                resolved.getFragment());
    }

    /**
     * Checks if the provided path string is either null or has zero length and throws a {@link
     * IllegalArgumentException} if any of the two conditions apply.
     *
     * @param path the path string to be checked
     */
    private void checkPathArg(String path) {
        // disallow construction of a Path from an empty string
        if (path == null) {
            throw new IllegalArgumentException("Can not create a Path from a null string");
        }
        if (path.length() == 0) {
            throw new IllegalArgumentException("Can not create a Path from an empty string");
        }
    }

    /**
     * Construct a path from a String. Path strings are URIs, but with unescaped elements and some
     * additional normalization.
     *
     * @param pathString the path string
     */
    public Path(String pathString) {
        checkPathArg(pathString);

        // We can't use 'new URI(String)' directly, since it assumes things are
        // escaped, which we don't require of Paths.

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(pathString) && pathString.charAt(0) != SEPARATOR_CHAR) {
            pathString = SEPARATOR + pathString;
        }

        // parse uri components
        String scheme = null;
        String authority = null;

        int start = 0;

        // parse uri scheme, if any
        int colon = pathString.indexOf(':');
        int slash = pathString.indexOf(SEPARATOR_CHAR);
        if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a scheme
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        // parse uri authority, if any
        if (pathString.startsWith("//", start)
                && (pathString.length() - start > 2)) { // has authority
            int nextSlash = pathString.indexOf(SEPARATOR_CHAR, start + 2);
            int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start + 2, authEnd);
            start = authEnd;
        }

        // uri path is the rest of the string -- query & fragment not supported
        String path = pathString.substring(start);

        initialize(scheme, authority, path, null);
    }

    /**
     * Construct a path from a URI.
     *
     * @param aUri the source URI
     */
    public Path(URI aUri) {
        uri = aUri.normalize();
    }

    /**
     * Construct a Path from a scheme, an authority and a path string.
     *
     * @param scheme the scheme
     * @param authority the authority
     * @param path the path
     */
    public Path(String scheme, String authority, String path) {
        checkPathArg(path);

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(path) && path.charAt(0) != SEPARATOR_CHAR) {
            path = SEPARATOR + path;
        }

        // add "./" in front of Linux relative paths so that a path containing
        // a colon e.q. "a:b" will not be interpreted as scheme "a".
        if (!WINDOWS && path.charAt(0) != SEPARATOR_CHAR) {
            path = CUR_DIR + SEPARATOR + path;
        }

        initialize(scheme, authority, path, null);
    }

    /**
     * Initializes a path object given the scheme, authority, path and fragment string.
     *
     * @param scheme the scheme string.
     * @param authority the authority string.
     * @param path the path string.
     * @param fragment the fragment string.
     */
    private void initialize(String scheme, String authority, String path, String fragment) {
        try {
            this.uri =
                    new URI(scheme, authority, normalizePath(scheme, path), null, fragment)
                            .normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Normalize a path string to use non-duplicated forward slashes as the path separator and
     * remove any trailing path separators.
     *
     * @param scheme the URI scheme. Used to deduce whether we should replace backslashes or not
     * @param path the scheme-specific part
     * @return the normalized path string
     */
    private static String normalizePath(String scheme, String path) {
        // Remove duplicated slashes.
        path = SLASHES.matcher(path).replaceAll(SEPARATOR);

        // Remove backslashes if this looks like a Windows path. Avoid
        // the substitution if it looks like a non-local URI.
        if (WINDOWS
                && (hasWindowsDrive(path)
                        || (scheme == null)
                        || (scheme.isEmpty())
                        || (scheme.equals("file")))) {
            path = StringUtils.replace(path, "\\", SEPARATOR);
        }

        // trim trailing slash from non-root path (ignoring windows drive)
        int minLength = startPositionWithoutWindowsDrive(path) + 1;
        if (path.length() > minLength && path.endsWith(SEPARATOR)) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }

    /**
     * Checks if the provided path string contains a windows drive letter.
     *
     * @return True, if the path string contains a windows drive letter, false otherwise.
     */
    private static boolean hasWindowsDrive(String path) {
        return (WINDOWS && HAS_DRIVE_LETTER_SPECIFIER.matcher(path).find());
    }

    /**
     * Get start position without windows drive letter.
     *
     * @param path the path string.
     * @return the start position of without windows drive letter.
     */
    private static int startPositionWithoutWindowsDrive(String path) {
        if (hasWindowsDrive(path)) {
            return path.charAt(0) == SEPARATOR_CHAR ? 3 : 2;
        } else {
            return 0;
        }
    }

    /**
     * Convert this Path to a URI.
     *
     * @return this Path as a URI
     */
    public URI toUri() {
        return uri;
    }

    /**
     * Returns the final component of this path, i.e., everything that follows the last separator.
     *
     * @return the final component of the path
     */
    public String getName() {
        String path = uri.getPath();
        int slash = path.lastIndexOf(SEPARATOR);
        return path.substring(slash + 1);
    }

    /** Create a temporary path (to be used as a copy) for this path. */
    public Path createTempPath() {
        return new Path(getParent(), String.format(".%s.%s.tmp", getName(), UUID.randomUUID()));
    }

    /**
     * Returns the parent of a path or null if at root.
     *
     * @return the parent of a path or null if at root
     */
    public Path getParent() {
        String path = uri.getPath();
        int lastSlash = path.lastIndexOf(SEPARATOR_CHAR);
        int start = startPositionWithoutWindowsDrive(path);
        if ((path.length() == start)
                || // empty path
                (lastSlash == start && path.length() == start + 1)) { // at root
            return null;
        }
        String parent;
        if (lastSlash == -1) {
            parent = CUR_DIR;
        } else {
            parent = path.substring(0, lastSlash == start ? start + 1 : lastSlash);
        }
        return new Path(uri.getScheme(), uri.getAuthority(), parent);
    }

    @Override
    public String toString() {
        // we can't use uri.toString(), which escapes everything, because we want
        // illegal characters unescaped in the string, for glob processing, etc.
        StringBuilder buffer = new StringBuilder();
        if (uri.getScheme() != null) {
            buffer.append(uri.getScheme()).append(":");
        }
        if (uri.getAuthority() != null) {
            buffer.append("//").append(uri.getAuthority());
        }
        if (uri.getPath() != null) {
            String path = uri.getPath();
            if (path.indexOf(SEPARATOR_CHAR) == 0
                    && hasWindowsDrive(path)
                    && // has windows drive
                    uri.getScheme() == null
                    && // but no scheme
                    uri.getAuthority() == null) {
                path = path.substring(1); // remove slash before drive
            }
            buffer.append(path);
        }
        if (uri.getFragment() != null) {
            buffer.append("#").append(uri.getFragment());
        }
        return buffer.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Path)) {
            return false;
        }
        Path that = (Path) o;
        return this.uri.equals(that.uri);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    @Override
    public int compareTo(Path that) {
        return this.uri.compareTo(that.uri);
    }
}
