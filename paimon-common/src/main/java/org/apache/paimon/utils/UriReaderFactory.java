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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** A factory to create and cache {@link UriReader}. */
public class UriReaderFactory implements Serializable {

    private final CatalogContext context;
    private final Map<UriKey, UriReader> readers;

    public UriReaderFactory(CatalogContext context) {
        this.context = context;
        this.readers = new ConcurrentHashMap<>();
    }

    public UriReader create(String input) {
        URI uri = URI.create(input);
        UriKey key = new UriKey(uri.getScheme(), uri.getAuthority());
        return readers.computeIfAbsent(key, k -> newReader(k, uri));
    }

    private UriReader newReader(UriKey key, URI uri) {
        if ("http".equals(key.scheme) || "https".equals(key.scheme)) {
            return UriReader.fromHttp();
        }

        try {
            FileIO fileIO = FileIO.get(new Path(uri), context);
            return UriReader.fromFile(fileIO);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
