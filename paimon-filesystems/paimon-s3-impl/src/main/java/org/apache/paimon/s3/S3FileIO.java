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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.StorageType;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.RestoreRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.paimon.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    @Override
    public Optional<Path> archive(Path path, StorageType type) throws IOException {
        if (!isObjectStore()) {
            throw new UnsupportedOperationException(
                    "Archive operation is only supported for object stores");
        }

        org.apache.hadoop.fs.Path hadoopPath = path(path);
        S3AFileSystem fs = (S3AFileSystem) getFileSystem(hadoopPath);

        try {
            String storageClass = mapStorageTypeToS3StorageClass(type);
            archivePath(fs, hadoopPath, storageClass);
            // S3 archiving is in-place, path doesn't change
            return Optional.empty();
        } catch (Exception e) {
            throw new IOException("Failed to archive path: " + path, e);
        }
    }

    @Override
    public void restoreArchive(Path path, Duration duration) throws IOException {
        if (!isObjectStore()) {
            throw new UnsupportedOperationException(
                    "Restore archive operation is only supported for object stores");
        }

        org.apache.hadoop.fs.Path hadoopPath = path(path);
        S3AFileSystem fs = (S3AFileSystem) getFileSystem(hadoopPath);

        try {
            // For S3 Glacier/Deep Archive, we need to initiate a restore request
            AmazonS3 s3Client = getS3Client(fs);
            if (s3Client != null) {
                URI uri = hadoopPath.toUri();
                String bucket = uri.getHost();
                String key = uri.getPath();
                if (key.startsWith("/")) {
                    key = key.substring(1);
                }

                // Check if the object is in Glacier or Deep Archive
                ObjectMetadata metadata = s3Client.getObjectMetadata(bucket, key);
                String storageClass = metadata.getStorageClass();
                
                if (StorageClass.Glacier.toString().equals(storageClass)
                        || StorageClass.DeepArchive.toString().equals(storageClass)) {
                    // Initiate restore request
                    RestoreObjectRequest restoreRequest = new RestoreObjectRequest(bucket, key);
                    
                    // Set restore tier and days
                    // Expedited: 1-5 minutes, Standard: 3-5 hours, Bulk: 5-12 hours
                    // For Deep Archive: Standard (12 hours) or Bulk (48 hours)
                    int days = (int) duration.toDays();
                    if (days <= 0) {
                        days = 7; // Default to 7 days
                    }
                    
                    if (StorageClass.DeepArchive.toString().equals(storageClass)) {
                        // Deep Archive only supports Standard or Bulk tier
                        restoreRequest.setRestoreRequest(new RestoreRequest(days, Tier.Standard));
                    } else {
                        // Glacier supports Expedited, Standard, or Bulk
                        restoreRequest.setRestoreRequest(new RestoreRequest(days, Tier.Standard));
                    }
                    
                    s3Client.restoreObjectV2(restoreRequest);
                    LOG.info(
                            "Initiated restore request for s3://{}/{} (storage class: {}, duration: {} days)",
                            bucket,
                            key,
                            storageClass,
                            days);
                } else {
                    LOG.debug(
                            "Object s3://{}/{} is not in archive storage (storage class: {}), "
                                    + "no restore needed",
                            bucket,
                            key,
                            storageClass);
                }
            } else {
                // Fallback: log and let S3AFileSystem handle restore on access
                LOG.debug(
                        "S3 client not accessible, restore will happen automatically on access. "
                                + "Path: {}, duration: {}",
                        path,
                        duration);
            }
        } catch (Exception e) {
            throw new IOException("Failed to restore archive for path: " + path, e);
        }
    }

    @Override
    public Optional<Path> unarchive(Path path, StorageType type) throws IOException {
        if (!isObjectStore()) {
            throw new UnsupportedOperationException(
                    "Unarchive operation is only supported for object stores");
        }

        org.apache.hadoop.fs.Path hadoopPath = path(path);
        S3AFileSystem fs = (S3AFileSystem) getFileSystem(hadoopPath);

        try {
            // Move back to STANDARD storage class
            archivePath(fs, hadoopPath, "STANDARD");
            // S3 unarchiving is in-place, path doesn't change
            return Optional.empty();
        } catch (Exception e) {
            throw new IOException("Failed to unarchive path: " + path, e);
        }
    }

    /**
     * Archive a path (file or directory) recursively to the specified S3 storage class.
     *
     * @param fs the S3AFileSystem instance
     * @param hadoopPath the path to archive
     * @param storageClass the S3 storage class to use
     */
    private void archivePath(S3AFileSystem fs, org.apache.hadoop.fs.Path hadoopPath, String storageClass)
            throws IOException {
        if (!fs.exists(hadoopPath)) {
            throw new IOException("Path does not exist: " + hadoopPath);
        }

        org.apache.hadoop.fs.FileStatus status = fs.getFileStatus(hadoopPath);
        if (status.isDirectory()) {
            // Archive all files in the directory recursively
            org.apache.hadoop.fs.FileStatus[] children = fs.listStatus(hadoopPath);
            for (org.apache.hadoop.fs.FileStatus child : children) {
                archivePath(fs, child.getPath(), storageClass);
            }
        } else {
            // Archive the file by changing its storage class
            changeStorageClass(fs, hadoopPath, storageClass);
        }
    }

    /**
     * Change the storage class of an S3 object using AWS SDK CopyObjectRequest.
     *
     * <p>This method uses CopyObjectRequest to copy the object to itself with a new storage class,
     * which effectively changes the storage class in-place without changing the object path.
     *
     * @param fs the S3AFileSystem instance
     * @param hadoopPath the path to the object
     * @param storageClass the target storage class
     */
    private void changeStorageClass(S3AFileSystem fs, org.apache.hadoop.fs.Path hadoopPath, String storageClass)
            throws IOException {
        try {
            // Get the S3 client from S3AFileSystem using reflection
            AmazonS3 s3Client = getS3Client(fs);
            if (s3Client == null) {
                throw new IOException(
                        "Unable to access S3 client from S3AFileSystem. "
                                + "Storage class change requires direct S3 client access.");
            }

            URI uri = hadoopPath.toUri();
            String bucket = uri.getHost();
            String key = uri.getPath();
            if (key.startsWith("/")) {
                key = key.substring(1);
            }

            // Use CopyObjectRequest to copy object to itself with new storage class
            // This is the standard way to change storage class in-place
            CopyObjectRequest copyRequest = new CopyObjectRequest(bucket, key, bucket, key);
            copyRequest.setStorageClass(StorageClass.fromValue(storageClass));
            
            // Preserve metadata by copying it
            ObjectMetadata metadata = s3Client.getObjectMetadata(bucket, key);
            copyRequest.setNewObjectMetadata(metadata);

            s3Client.copyObject(copyRequest);

            LOG.debug(
                    "Successfully changed storage class for s3://{}/{} to {}",
                    bucket,
                    key,
                    storageClass);
        } catch (Exception e) {
            throw new IOException(
                    "Failed to change storage class for " + hadoopPath + " to " + storageClass, e);
        }
    }

    /**
     * Get the AmazonS3 client from S3AFileSystem using reflection.
     *
     * @param fs the S3AFileSystem instance
     * @return the AmazonS3 client, or null if not accessible
     */
    private AmazonS3 getS3Client(S3AFileSystem fs) {
        try {
            // Try to get the S3 client from S3AFileSystem's store
            // S3AFileSystem has a getAmazonS3Client() method in some versions
            // or we can access it through the store
            Object store = ReflectionUtils.getPrivateFieldValue(fs, "store");
            if (store != null) {
                // Try to get the S3 client from the store
                try {
                    return (AmazonS3) ReflectionUtils.getPrivateFieldValue(store, "s3");
                } catch (Exception e) {
                    // Try alternative field names
                    try {
                        return (AmazonS3) ReflectionUtils.getPrivateFieldValue(store, "client");
                    } catch (Exception e2) {
                        try {
                            return (AmazonS3) ReflectionUtils.getPrivateFieldValue(store, "s3Client");
                        } catch (Exception e3) {
                            // Try calling getAmazonS3Client() method if available
                            try {
                                return (AmazonS3)
                                        ReflectionUtils.invokeMethod(store, "getAmazonS3Client");
                            } catch (Exception e4) {
                                LOG.debug("Could not access S3 client from store", e4);
                            }
                        }
                    }
                }
            }

            // Try direct method call on S3AFileSystem
            try {
                return (AmazonS3) ReflectionUtils.invokeMethod(fs, "getAmazonS3Client");
            } catch (Exception e) {
                LOG.debug("Could not access S3 client via getAmazonS3Client()", e);
            }

            return null;
        } catch (Exception e) {
            LOG.warn("Failed to get S3 client from S3AFileSystem", e);
            return null;
        }
    }

    /**
     * Map Paimon StorageType to S3 storage class name.
     *
     * @param type the Paimon storage type
     * @return the S3 storage class name
     */
    private String mapStorageTypeToS3StorageClass(StorageType type) {
        switch (type) {
            case Standard:
                return "STANDARD";
            case Archive:
                return "GLACIER";
            case ColdArchive:
                return "DEEP_ARCHIVE";
            default:
                throw new IllegalArgumentException("Unsupported storage type: " + type);
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
