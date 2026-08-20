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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A factory to create and cache {@link UriReader}. */
public class UriReaderFactory implements Serializable {

    private static final long serialVersionUID = -8477284718943635074L;
    private static final ConfigOption<Duration> HTTP_KEEP_ALIVE_TIMEOUT =
            ConfigOptions.key(
                            CoreOptions.BLOB_DESCRIPTOR_HTTP_KEEP_ALIVE_TIMEOUT
                                    .key()
                                    .substring(CoreOptions.BLOB_DESCRIPTOR_PREFIX.length()))
                    .durationType()
                    .noDefaultValue();

    @Nullable private final CatalogContext context;
    @Nullable private final Duration httpKeepAliveTimeout;
    private transient Map<UriKey, UriReader> readers;

    public UriReaderFactory(@Nullable CatalogContext context) {
        this(context, keepAliveTimeout(context));
    }

    public UriReaderFactory(
            @Nullable CatalogContext context, @Nullable Duration httpKeepAliveTimeout) {
        this.context = context;
        validateKeepAliveTimeout(httpKeepAliveTimeout);
        this.httpKeepAliveTimeout = httpKeepAliveTimeout;
        this.readers = new ConcurrentHashMap<>();
    }

    /** Creates a factory which uses the provided {@link FileIO} for non-HTTP URIs. */
    public static UriReaderFactory fromFileIO(FileIO fileIO) {
        return new ProvidedFileIOUriReaderFactory(fileIO, null);
    }

    /**
     * Creates a factory with a keep-alive timeout for HTTP URIs and the provided {@link FileIO}.
     */
    public static UriReaderFactory fromFileIO(FileIO fileIO, Duration httpKeepAliveTimeout) {
        return new ProvidedFileIOUriReaderFactory(fileIO, httpKeepAliveTimeout);
    }

    public UriReader create(String input) {
        URI uri = parseUri(input);
        UriKey key = new UriKey(uri.getScheme(), uri.getAuthority());
        return readers.computeIfAbsent(key, k -> newReader(uri));
    }

    private static URI parseUri(String input) {
        try {
            return URI.create(input);
        } catch (IllegalArgumentException e) {
            // File paths may contain unescaped characters accepted by Path.
            try {
                URI pathUri = new Path(input).toUri();
                if (!isHttp(pathUri)) {
                    return pathUri;
                }
            } catch (IllegalArgumentException ignored) {
                // Throw the sanitized exception below.
            }
            throw SensitiveConfigUtils.invalidUri(input);
        }
    }

    public boolean exists(String input) throws IOException {
        UriReader reader = create(input);
        if (reader instanceof UriReader.FileUriReader) {
            return ((UriReader.FileUriReader) reader).exists(input);
        }
        if (reader instanceof UriReader.HttpUriReader) {
            return ((UriReader.HttpUriReader) reader).exists(input);
        }
        return true;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.readers = new ConcurrentHashMap<>();
    }

    protected UriReader newReader(URI uri) {
        if (isHttp(uri)) {
            return httpKeepAliveTimeout == null
                    ? UriReader.fromHttp()
                    : UriReader.fromHttp(httpKeepAliveTimeout);
        }

        try {
            FileIO createdFileIO = FileIO.get(new Path(uri), Objects.requireNonNull(context));
            return UriReader.fromFile(createdFileIO);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isHttp(URI uri) {
        return "http".equalsIgnoreCase(uri.getScheme())
                || "https".equalsIgnoreCase(uri.getScheme());
    }

    @Nullable
    private static Duration keepAliveTimeout(@Nullable CatalogContext context) {
        return context == null
                ? null
                : context.options().getOptional(HTTP_KEEP_ALIVE_TIMEOUT).orElse(null);
    }

    private static void validateKeepAliveTimeout(@Nullable Duration keepAliveTimeout) {
        if (keepAliveTimeout != null) {
            checkArgument(
                    !keepAliveTimeout.isZero() && !keepAliveTimeout.isNegative(),
                    "Option '%s' must be greater than 0.",
                    HTTP_KEEP_ALIVE_TIMEOUT.key());
        }
    }

    private static final class ProvidedFileIOUriReaderFactory extends UriReaderFactory {

        private static final long serialVersionUID = 1L;

        // Intentionally not transient. FileIO is serializable by contract, while implementations
        // keep process-local clients transient. Distributed workers need this serialized FileIO to
        // rebuild the transient reader cache with table-scoped credentials.
        private final FileIO fileIO;

        private ProvidedFileIOUriReaderFactory(
                FileIO fileIO, @Nullable Duration httpKeepAliveTimeout) {
            super(null, httpKeepAliveTimeout);
            this.fileIO = Objects.requireNonNull(fileIO);
        }

        @Override
        protected UriReader newReader(URI uri) {
            return isHttp(uri) ? super.newReader(uri) : UriReader.fromFile(fileIO);
        }
    }

    private static final class UriKey {

        private final @Nullable String scheme;
        private final @Nullable String authority;

        public UriKey(@Nullable String scheme, @Nullable String authority) {
            this.scheme = scheme;
            this.authority = authority;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UriKey uriKey = (UriKey) o;
            return Objects.equals(scheme, uriKey.scheme)
                    && Objects.equals(authority, uriKey.authority);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scheme, authority);
        }
    }
}
