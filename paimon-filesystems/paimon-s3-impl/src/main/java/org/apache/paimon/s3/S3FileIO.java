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
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3ADataBlocks;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.statistics.impl.StubDurationTrackerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** S3 {@link FileIO}. */
public class S3FileIO extends HadoopCompliantFileIO {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(S3FileIO.class);

    private static final String[] CONFIG_PREFIXES = {"s3.", "s3a.", "fs.s3a."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.s3a.";

    private static final String SNAPSHOT_DIRECTORY = "snapshot";

    private static final String SNAPSHOT_FILE_PREFIX = "snapshot-";

    private static final String LATEST = "LATEST";

    private static final String EARLIEST = "EARLIEST";

    private static final String IF_NONE_MATCH_ANY = "*";

    private static final String[][] MIRRORED_CONFIG_KEYS = {
        {"fs.s3a.access-key", "fs.s3a.access.key"},
        {"fs.s3a.secret-key", "fs.s3a.secret.key"},
        {"fs.s3a.path-style-access", "fs.s3a.path.style.access"},
        {"fs.s3a.signer-type", "fs.s3a.signing-algorithm"}
    };

    /**
     * Cache S3AFileSystem, at present, there is no good mechanism to ensure that the file system
     * will be shut down, so here the fs cache is used to avoid resource leakage.
     */
    private static final Map<CacheKey, S3AFileSystem> CACHE = new ConcurrentHashMap<>();

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
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        if (isSnapshotMetadataFile(path)) {
            return newDirectOutputStream(path, overwrite);
        }
        return super.newOutputStream(path, overwrite);
    }

    @Override
    public boolean supportsAtomicCreateWithoutOverwrite(Path path) {
        return isSnapshotMetadataFile(path);
    }

    protected PositionOutputStream newDirectOutputStream(Path path, boolean overwrite)
            throws IOException {
        return new DirectS3OutputStream(path, overwrite);
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

    static PutObjectRequest applyOverwriteMode(PutObjectRequest request, boolean overwrite) {
        if (overwrite) {
            return request;
        }
        return request.toBuilder().ifNoneMatch(IF_NONE_MATCH_ANY).build();
    }

    static boolean isPreconditionFailed(Throwable throwable) {
        while (throwable != null) {
            if (throwable instanceof S3Exception) {
                int statusCode = ((S3Exception) throwable).statusCode();
                return statusCode == 409 || statusCode == 412;
            }
            throwable = throwable.getCause();
        }
        return false;
    }

    private static boolean isSnapshotMetadataFile(Path path) {
        Path parent = path.getParent();
        if (parent == null || !SNAPSHOT_DIRECTORY.equals(parent.getName())) {
            return false;
        }

        String fileName = path.getName();
        return fileName.startsWith(SNAPSHOT_FILE_PREFIX)
                || LATEST.equals(fileName)
                || EARLIEST.equals(fileName);
    }

    private static FileAlreadyExistsException fileAlreadyExists(Path path, Throwable cause) {
        FileAlreadyExistsException exception = new FileAlreadyExistsException(path.toString());
        exception.initCause(cause);
        return exception;
    }

    @Override
    protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) {
        final String scheme = path.toUri().getScheme();
        final String authority = path.toUri().getAuthority();
        return CACHE.computeIfAbsent(
                new CacheKey(hadoopOptions, scheme, authority),
                key -> {
                    Configuration hadoopConf = new Configuration();
                    key.options.toMap().forEach(hadoopConf::set);
                    URI fsUri = path.toUri();
                    if (scheme == null && authority == null) {
                        fsUri = FileSystem.getDefaultUri(hadoopConf);
                    } else if (scheme != null && authority == null) {
                        URI defaultUri = FileSystem.getDefaultUri(hadoopConf);
                        if (scheme.equals(defaultUri.getScheme())
                                && defaultUri.getAuthority() != null) {
                            fsUri = defaultUri;
                        }
                    }

                    S3AFileSystem fs = new S3AFileSystem();
                    try {
                        fs.initialize(fsUri, hadoopConf);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return fs;
                });
    }

    private class DirectS3OutputStream extends PositionOutputStream {

        private final Path path;
        private final boolean overwrite;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private boolean closed;

        private DirectS3OutputStream(Path path, boolean overwrite) {
            this.path = path;
            this.overwrite = overwrite;
        }

        @Override
        public long getPos() throws IOException {
            checkOpen();
            return buffer.size();
        }

        @Override
        public void write(int b) throws IOException {
            checkOpen();
            buffer.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            checkOpen();
            buffer.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            checkOpen();
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }

            byte[] data = buffer.toByteArray();
            org.apache.hadoop.fs.Path hadoopPath = path(path);
            S3AFileSystem fs = (S3AFileSystem) getFileSystem(hadoopPath);
            WriteOperationHelper writeHelper =
                    fs.createWriteOperationHelper(fs.getActiveAuditSpan());
            PutObjectOptions options = PutObjectOptions.defaultOptions();
            PutObjectRequest request =
                    applyOverwriteMode(
                            writeHelper.createPutObjectRequest(
                                    fs.pathToKey(hadoopPath), data.length, options),
                            overwrite);

            try (S3ADataBlocks.BlockUploadData uploadData =
                    new S3ADataBlocks.BlockUploadData(data, () -> !closed)) {
                writeHelper.putObject(
                        request,
                        options,
                        uploadData,
                        StubDurationTrackerFactory.STUB_DURATION_TRACKER_FACTORY);
                closed = true;
            } catch (IOException e) {
                if (isPreconditionFailed(e)) {
                    throw fileAlreadyExists(path, e);
                }
                throw e;
            }
        }

        private void checkOpen() throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
        }
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
