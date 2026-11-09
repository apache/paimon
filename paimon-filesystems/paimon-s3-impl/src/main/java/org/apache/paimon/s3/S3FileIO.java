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

package org.apache.paimon.s3;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** S3 {@link FileIO}. */
public class S3FileIO extends HadoopCompliantFileIO {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(S3FileIO.class);

    private static final String[] CONFIG_PREFIXES = {"s3.", "s3a.", "fs.s3a."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.s3a.";

    private static final String[][] MIRRORED_CONFIG_KEYS = {
        {"fs.s3a.access-key", "fs.s3a.access.key"},
        {"fs.s3a.secret-key", "fs.s3a.secret.key"},
        {"fs.s3a.session-token", "fs.s3a.session.token"},
        {"fs.s3a.path-style-access", "fs.s3a.path.style.access"},
        {"fs.s3a.signer-type", "fs.s3a.signing-algorithm"}
    };

    /**
     * Shared cache of {@link S3AFileSystem} instances across {@link S3FileIO} objects. Sharing
     * instances avoids creating duplicate filesystem instances for the same cache key. There is
     * currently no reference-counted shutdown for published entries. Pool-shutdown recovery only
     * evicts an unusable mapping from this cache; it does not close a previously published
     * filesystem that may still be referenced by other {@code S3FileIO} {@code fsMap} entries or
     * in-flight streams.
     */
    private static final Map<CacheKey, S3AFileSystem> CACHE = new ConcurrentHashMap<>();

    /**
     * Per-CacheKey locks: create/replace of the same key is serialized; different keys may create
     * concurrently. Invalidation stays lock-free and does not remove these locks.
     */
    private static final Map<CacheKey, Object> CACHE_LOCKS = new ConcurrentHashMap<>();

    private Options hadoopOptions;

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public void configure(CatalogContext context) {
        this.hadoopOptions = mirrorCertainHadoopConfig(loadHadoopConfigFromContext(context));
    }

    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        S3AFileSystem fs = (S3AFileSystem) getFileSystem(hadoopPath);
        if (!overwrite && this.exists(path)) {
            throw new IOException("File " + path + " already exists.");
        }
        return new S3TwoPhaseOutputStream(
                new S3MultiPartUpload(fs, fs.getConf()), hadoopPath, path);
    }

    // add additional config entries from the IO config to the Hadoop config
    private Options loadHadoopConfigFromContext(CatalogContext context) {
        Options hadoopConfig = new Options();
        for (String key : context.options().keySet()) {
            for (String prefix : CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String newKey = HADOOP_CONFIG_PREFIX + key.substring(prefix.length());
                    String value = context.options().get(key);
                    hadoopConfig.set(newKey, value);

                    LOG.debug("Adding config entry for {} as {} to Hadoop config", key, newKey);
                }
            }
        }
        return hadoopConfig;
    }

    // mirror certain keys to make use more uniform across implementations
    // with different keys
    private Options mirrorCertainHadoopConfig(Options hadoopConfig) {
        for (String[] mirrored : MIRRORED_CONFIG_KEYS) {
            String value = hadoopConfig.get(mirrored[0]);
            if (value != null) {
                hadoopConfig.set(mirrored[1], value);
            }
        }
        return hadoopConfig;
    }

    /**
     * Overrides parent to refresh {@link #fsMap} when a cached {@link RecoverableS3AFileSystem} has
     * been invalidated. Avoids introducing a generic health hook on {@link HadoopCompliantFileIO}.
     */
    @Override
    protected FileSystem getFileSystem(org.apache.hadoop.fs.Path path) throws IOException {
        if (fsMap == null) {
            synchronized (this) {
                if (fsMap == null) {
                    fsMap = new ConcurrentHashMap<>();
                }
            }
        }

        final Map<Pair<String, String>, FileSystem> map = fsMap;
        URI uri = path.toUri();
        Pair<String, String> key = Pair.of(uri.getScheme(), uri.getAuthority());

        // Create outside fsMap's ConcurrentHashMap compute lock.
        while (true) {
            FileSystem current = map.get(key);
            if (isUsableFileSystem(current)) {
                return current;
            }

            FileSystem created = createFileSystem(path);
            if (!isUsableFileSystem(created)) {
                continue;
            }

            if (current == null) {
                FileSystem existing = map.putIfAbsent(key, created);
                if (existing == null) {
                    if (isUsableFileSystem(created)) {
                        return created;
                    }
                    map.remove(key, created);
                    continue;
                }
                if (isUsableFileSystem(existing)) {
                    return existing;
                }
                current = existing;
            }

            if (isUsableFileSystem(current)) {
                return current;
            }
            if (!map.replace(key, current, created)) {
                continue;
            }
            if (isUsableFileSystem(created)) {
                return created;
            }
            map.remove(key, created);
        }
    }

    @Override
    protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) throws IOException {
        final String scheme = path.toUri().getScheme();
        final String authority = path.toUri().getAuthority();
        final CacheKey cacheKey = new CacheKey(hadoopOptions, scheme, authority);

        S3AFileSystem cached = CACHE.get(cacheKey);
        if (isUsableFileSystem(cached)) {
            return cached;
        }

        Object lock = getCacheLock(cacheKey);
        synchronized (lock) {
            cached = CACHE.get(cacheKey);
            if (isUsableFileSystem(cached)) {
                return cached;
            }

            // Only this confirmed mapping may be removed if initialize fails.
            final S3AFileSystem expectedOld = cached;

            Configuration hadoopConf = new Configuration();
            cacheKey.options.toMap().forEach(hadoopConf::set);
            URI fsUri = path.toUri();
            if (scheme == null && authority == null) {
                fsUri = FileSystem.getDefaultUri(hadoopConf);
            } else if (scheme != null && authority == null) {
                URI defaultUri = FileSystem.getDefaultUri(hadoopConf);
                if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
                    fsUri = defaultUri;
                }
            }

            RecoverableS3AFileSystem fs = newRecoverableFileSystem();
            fs.bind(() -> CACHE.remove(cacheKey, fs));
            try {
                fs.initialize(fsUri, hadoopConf);
                if (!fs.isUsable()) {
                    throw new IOException("S3A filesystem became unusable during initialize");
                }
            } catch (IOException | RuntimeException | Error e) {
                discardFailedCreate(cacheKey, expectedOld, fs);
                throw e;
            }

            CACHE.put(cacheKey, fs);
            if (!fs.isUsable()) {
                CACHE.remove(cacheKey, fs);
                try {
                    fs.close();
                } catch (Throwable t) {
                    LOG.warn(
                            "Failed to close S3AFileSystem that became unusable after cache publish",
                            t);
                }
                throw new IOException(
                        "S3A filesystem became unusable before cache publish completed");
            }
            return fs;
        }
    }

    RecoverableS3AFileSystem newRecoverableFileSystem() {
        return new RecoverableS3AFileSystem();
    }

    /**
     * Cleans up after a failed create: remove only {@code expectedOld} if it is still cached, and
     * close the unpublished filesystem. Never removes a newer published instance.
     */
    private static void discardFailedCreate(
            CacheKey cacheKey, S3AFileSystem expectedOld, S3AFileSystem unpublished) {
        if (expectedOld != null) {
            CACHE.remove(cacheKey, expectedOld);
        }
        try {
            unpublished.close();
        } catch (Throwable t) {
            LOG.warn("Failed to close unpublished S3AFileSystem after initialize failure", t);
        }
    }

    private static Object getCacheLock(CacheKey key) {
        return CACHE_LOCKS.computeIfAbsent(key, k -> new Object());
    }

    static boolean isUsableFileSystem(FileSystem fs) {
        if (fs == null) {
            return false;
        }
        if (fs instanceof RecoverableS3AFileSystem) {
            return ((RecoverableS3AFileSystem) fs).isUsable();
        }
        return true;
    }

    static void clearCache() {
        CACHE.clear();
    }

    private static class CacheKey {

        private final Options options;
        private final String scheme;
        private final String authority;

        private CacheKey(Options options, String scheme, String authority) {
            this.options = options;
            this.scheme = scheme;
            this.authority = authority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(options, cacheKey.options)
                    && Objects.equals(scheme, cacheKey.scheme)
                    && Objects.equals(authority, cacheKey.authority);
        }

        @Override
        public int hashCode() {
            return Objects.hash(options, scheme, authority);
        }
    }
}
