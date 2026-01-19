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

package org.apache.paimon.oss;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.StorageType;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.ReflectionUtils;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.comm.ServiceClient;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.RestoreObjectRequest;
import com.aliyun.oss.model.StorageClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;

/** OSS {@link FileIO}. */
public class OSSFileIO extends HadoopCompliantFileIO {

    private static final long serialVersionUID = 2L;

    private static final Logger LOG = LoggerFactory.getLogger(OSSFileIO.class);

    /**
     * In order to simplify, we make paimon oss configuration keys same with hadoop oss module. So,
     * we add all configuration key with prefix `fs.oss` in paimon conf to hadoop conf.
     */
    private static final String[] CONFIG_PREFIXES = {"fs.oss."};

    private static final String OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId";
    private static final String OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    private static final String OSS_SECURITY_TOKEN = "fs.oss.securityToken";
    private static final String OSS_SECOND_LEVEL_DOMAIN_ENABLED = "fs.oss.sld.enabled";

    private static final Map<String, String> CASE_SENSITIVE_KEYS =
            new HashMap<String, String>() {
                {
                    put(OSS_ACCESS_KEY_ID.toLowerCase(), OSS_ACCESS_KEY_ID);
                    put(OSS_ACCESS_KEY_SECRET.toLowerCase(), OSS_ACCESS_KEY_SECRET);
                    put(OSS_SECURITY_TOKEN.toLowerCase(), OSS_SECURITY_TOKEN);
                }
            };

    /**
     * Cache AliyunOSSFileSystem, at present, there is no good mechanism to ensure that the file
     * system will be shut down, so here the fs cache is used to avoid resource leakage.
     */
    private static final Map<CacheKey, AliyunOSSFileSystem> CACHE = new ConcurrentHashMap<>();

    // create a shared config to avoid load properties everytime
    private static final Configuration SHARED_CONFIG = new Configuration();

    private Options hadoopOptions;
    private boolean allowCache = true;

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public void configure(CatalogContext context) {
        allowCache = context.options().get(FILE_IO_ALLOW_CACHE);
        hadoopOptions = new Options();
        // read all configuration with prefix 'CONFIG_PREFIXES'
        for (String key : context.options().keySet()) {
            for (String prefix : CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String value = context.options().get(key);
                    if (CASE_SENSITIVE_KEYS.containsKey(key.toLowerCase())) {
                        key = CASE_SENSITIVE_KEYS.get(key.toLowerCase());
                    }
                    hadoopOptions.set(key, value);

                    LOG.debug(
                            "Adding config entry for {} as {} to Hadoop config",
                            key,
                            hadoopOptions.get(key));
                }
            }
        }
    }

    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        if (!overwrite && this.exists(path)) {
            throw new IOException("File " + path + " already exists.");
        }
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        FileSystem fs = getFileSystem(hadoopPath);
        return new OssTwoPhaseOutputStream(
                new OSSMultiPartUpload((org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem) fs),
                hadoopPath,
                path);
    }

    public Options hadoopOptions() {
        return hadoopOptions;
    }

    @Override
    protected AliyunOSSFileSystem createFileSystem(org.apache.hadoop.fs.Path path) {
        final String scheme = path.toUri().getScheme();
        final String authority = path.toUri().getAuthority();
        Supplier<AliyunOSSFileSystem> supplier =
                () -> {
                    // create config from base config, if initializing a new config, it will
                    // retrieve props from the file, which comes at a high cost
                    Configuration hadoopConf = new Configuration(SHARED_CONFIG);
                    hadoopOptions.toMap().forEach(hadoopConf::set);
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

                    AliyunOSSFileSystem fs = new AliyunOSSFileSystem();
                    try {
                        fs.initialize(fsUri, hadoopConf);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }

                    if (hadoopOptions.getBoolean(OSS_SECOND_LEVEL_DOMAIN_ENABLED, false)) {
                        enableSecondLevelDomain(fs);
                    }

                    return fs;
                };

        if (allowCache) {
            return CACHE.computeIfAbsent(
                    new CacheKey(hadoopOptions, scheme, authority), key -> supplier.get());
        } else {
            return supplier.get();
        }
    }

    @Override
    public void close() {
        if (!allowCache) {
            fsMap.values().forEach(IOUtils::closeQuietly);
            fsMap.clear();
        }
    }

    public void enableSecondLevelDomain(AliyunOSSFileSystem fs) {
        AliyunOSSFileSystemStore store = fs.getStore();
        try {
            OSSClient ossClient = ReflectionUtils.getPrivateFieldValue(store, "ossClient");
            ServiceClient serviceClient =
                    ReflectionUtils.getPrivateFieldValue(ossClient, "serviceClient");
            serviceClient.getClientConfiguration().setSLDEnabled(true);
        } catch (Exception e) {
            LOG.error("Failed to enable second level domain.", e);
            throw new RuntimeException("Failed to enable second level domain.", e);
        }
    }

    @Override
    public Optional<Path> archive(Path path, StorageType type) throws IOException {
        if (!isObjectStore()) {
            throw new UnsupportedOperationException(
                    "Archive operation is only supported for object stores");
        }

        org.apache.hadoop.fs.Path hadoopPath = path(path);
        AliyunOSSFileSystem fs = (AliyunOSSFileSystem) getFileSystem(hadoopPath);

        try {
            String storageClass = mapStorageTypeToOSSStorageClass(type);
            archivePath(fs, hadoopPath, storageClass);
            // OSS archiving is in-place, path doesn't change
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
        AliyunOSSFileSystem fs = (AliyunOSSFileSystem) getFileSystem(hadoopPath);

        try {
            // For OSS Archive/ColdArchive, we need to initiate a restore request
            OSSClient ossClient = getOSSClient(fs);
            if (ossClient != null) {
                URI uri = hadoopPath.toUri();
                String bucket = uri.getHost();
                String key = uri.getPath();
                if (key.startsWith("/")) {
                    key = key.substring(1);
                }

                // Check if the object is in Archive or ColdArchive
                ObjectMetadata metadata = ossClient.getObjectMetadata(bucket, key);
                StorageClass storageClass = metadata.getObjectStorageClass();

                if (storageClass == StorageClass.Archive || storageClass == StorageClass.ColdArchive) {
                    // Initiate restore request
                    RestoreObjectRequest restoreRequest = new RestoreObjectRequest(bucket, key);

                    // Set restore days (OSS restore duration is in days)
                    int days = (int) duration.toDays();
                    if (days <= 0) {
                        days = 7; // Default to 7 days
                    }
                    restoreRequest.setDays(days);

                    ossClient.restoreObject(restoreRequest);
                    LOG.info(
                            "Initiated restore request for oss://{}/{} (storage class: {}, duration: {} days)",
                            bucket,
                            key,
                            storageClass,
                            days);
                } else {
                    LOG.debug(
                            "Object oss://{}/{} is not in archive storage (storage class: {}), "
                                    + "no restore needed",
                            bucket,
                            key,
                            storageClass);
                }
            } else {
                // Fallback: log and let AliyunOSSFileSystem handle restore on access
                LOG.debug(
                        "OSS client not accessible, restore will happen automatically on access. "
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
        AliyunOSSFileSystem fs = (AliyunOSSFileSystem) getFileSystem(hadoopPath);

        try {
            // Move back to STANDARD storage class
            archivePath(fs, hadoopPath, "Standard");
            // OSS unarchiving is in-place, path doesn't change
            return Optional.empty();
        } catch (Exception e) {
            throw new IOException("Failed to unarchive path: " + path, e);
        }
    }

    /**
     * Archive a path (file or directory) recursively to the specified OSS storage class.
     *
     * @param fs the AliyunOSSFileSystem instance
     * @param hadoopPath the path to archive
     * @param storageClass the OSS storage class to use
     */
    private void archivePath(
            AliyunOSSFileSystem fs, org.apache.hadoop.fs.Path hadoopPath, String storageClass)
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
     * Change the storage class of an OSS object using OSS SDK CopyObjectRequest.
     *
     * <p>This method uses CopyObjectRequest to copy the object to itself with a new storage class,
     * which effectively changes the storage class in-place without changing the object path.
     *
     * @param fs the AliyunOSSFileSystem instance
     * @param hadoopPath the path to the object
     * @param storageClass the target storage class
     */
    private void changeStorageClass(
            AliyunOSSFileSystem fs, org.apache.hadoop.fs.Path hadoopPath, String storageClass)
            throws IOException {
        try {
            // Get the OSS client from AliyunOSSFileSystem using reflection
            OSSClient ossClient = getOSSClient(fs);
            if (ossClient == null) {
                throw new IOException(
                        "Unable to access OSS client from AliyunOSSFileSystem. "
                                + "Storage class change requires direct OSS client access.");
            }

            URI uri = hadoopPath.toUri();
            String bucket = uri.getHost();
            String key = uri.getPath();
            if (key.startsWith("/")) {
                key = key.substring(1);
            }

            // Map string storage class to OSS StorageClass enum
            StorageClass ossStorageClass = mapStringToOSSStorageClass(storageClass);

            // Use CopyObjectRequest to copy object to itself with new storage class
            // This is the standard way to change storage class in-place
            CopyObjectRequest copyRequest = new CopyObjectRequest(bucket, key, bucket, key);
            copyRequest.setNewObjectStorageClass(ossStorageClass);

            // Preserve metadata by copying it
            ObjectMetadata metadata = ossClient.getObjectMetadata(bucket, key);
            copyRequest.setNewObjectMetadata(metadata);

            ossClient.copyObject(copyRequest);

            LOG.debug(
                    "Successfully changed storage class for oss://{}/{} to {}",
                    bucket,
                    key,
                    storageClass);
        } catch (Exception e) {
            throw new IOException(
                    "Failed to change storage class for " + hadoopPath + " to " + storageClass, e);
        }
    }

    /**
     * Get the OSSClient from AliyunOSSFileSystem using reflection.
     *
     * @param fs the AliyunOSSFileSystem instance
     * @return the OSSClient, or null if not accessible
     */
    private OSSClient getOSSClient(AliyunOSSFileSystem fs) {
        try {
            AliyunOSSFileSystemStore store = fs.getStore();
            if (store != null) {
                // Get the OSS client from the store
                return ReflectionUtils.getPrivateFieldValue(store, "ossClient");
            }
            return null;
        } catch (Exception e) {
            LOG.warn("Failed to get OSS client from AliyunOSSFileSystem", e);
            return null;
        }
    }

    /**
     * Map string storage class name to OSS StorageClass enum.
     *
     * @param storageClass the storage class name
     * @return the OSS StorageClass enum value
     */
    private StorageClass mapStringToOSSStorageClass(String storageClass) {
        switch (storageClass) {
            case "Standard":
                return StorageClass.Standard;
            case "Archive":
                return StorageClass.Archive;
            case "ColdArchive":
                return StorageClass.ColdArchive;
            default:
                throw new IllegalArgumentException("Unsupported OSS storage class: " + storageClass);
        }
    }

    /**
     * Map Paimon StorageType to OSS storage class name.
     *
     * @param type the Paimon storage type
     * @return the OSS storage class name
     */
    private String mapStorageTypeToOSSStorageClass(StorageType type) {
        switch (type) {
            case Standard:
                return "Standard";
            case Archive:
                return "Archive";
            case ColdArchive:
                return "ColdArchive";
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
