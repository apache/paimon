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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.hadoop.HadoopFileIOLoader;
import org.apache.paimon.fs.local.LocalFileIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.fs.FileIOUtils.checkAccess;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * File IO to read and write file.
 *
 * @since 0.4.0
 */
@Public
@ThreadSafe
public interface FileIO extends Serializable {

    Logger LOG = LoggerFactory.getLogger(FileIO.class);

    boolean isObjectStore();

    /** Configure by {@link CatalogContext}. */
    void configure(CatalogContext context);

    /**
     * Opens an SeekableInputStream at the indicated Path.
     *
     * @param path the file to open
     */
    SeekableInputStream newInputStream(Path path) throws IOException;

    /**
     * Opens an PositionOutputStream at the indicated Path.
     *
     * @param path the file name to open
     * @param overwrite if a file with this name already exists, then if true, the file will be
     *     overwritten, and if false an error will be thrown.
     * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because a
     *     file already exists at that path and the write mode indicates to not overwrite the file.
     */
    PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException;

    /**
     * Return a file status object that represents the path.
     *
     * @param path The path we want information from
     * @return a FileStatus object
     * @throws FileNotFoundException when the path does not exist; IOException see specific
     *     implementation
     */
    FileStatus getFileStatus(Path path) throws IOException;

    /**
     * List the statuses of the files/directories in the given path if the path is a directory.
     *
     * @param path given path
     * @return the statuses of the files/directories in the given path
     */
    FileStatus[] listStatus(Path path) throws IOException;

    /**
     * List the statuses of the directories in the given path if the path is a directory.
     *
     * <p>{@link FileIO} implementation may have optimization for list directories.
     *
     * @param path given path
     * @return the statuses of the directories in the given path
     */
    default FileStatus[] listDirectories(Path path) throws IOException {
        FileStatus[] statuses = listStatus(path);
        if (statuses != null) {
            statuses = Arrays.stream(statuses).filter(FileStatus::isDir).toArray(FileStatus[]::new);
        }
        return statuses;
    }

    /**
     * Check if exists.
     *
     * @param path source file
     */
    boolean exists(Path path) throws IOException;

    /**
     * Delete a file.
     *
     * @param path the path to delete
     * @param recursive if path is a directory and set to <code>true</code>, the directory is
     *     deleted else throws an exception. In case of a file the recursive can be set to either
     *     <code>true</code> or <code>false</code>
     * @return <code>true</code> if delete is successful, <code>false</code> otherwise
     */
    boolean delete(Path path, boolean recursive) throws IOException;

    /**
     * Make the given file and all non-existent parents into directories. Has the semantics of Unix
     * 'mkdir -p'. Existence of the directory hierarchy is not an error.
     *
     * @param path the directory/directories to be created
     * @return <code>true</code> if at least one new directory has been created, <code>false</code>
     *     otherwise
     * @throws IOException thrown if an I/O error occurs while creating the directory
     */
    boolean mkdirs(Path path) throws IOException;

    /**
     * Renames the file/directory src to dst.
     *
     * @param src the file/directory to rename
     * @param dst the new name of the file/directory
     * @return <code>true</code> if the renaming was successful, <code>false</code> otherwise
     */
    boolean rename(Path src, Path dst) throws IOException;

    // -------------------------------------------------------------------------
    //                            utils
    // -------------------------------------------------------------------------

    default void deleteQuietly(Path file) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to delete " + file.toString());
        }

        try {
            if (!delete(file, false) && exists(file)) {
                LOG.warn("Failed to delete file " + file);
            }
        } catch (IOException e) {
            LOG.warn("Exception occurs when deleting file " + file, e);
        }
    }

    default void deleteDirectoryQuietly(Path directory) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to delete " + directory.toString());
        }

        try {
            if (!delete(directory, true) && exists(directory)) {
                LOG.warn("Failed to delete directory " + directory);
            }
        } catch (IOException e) {
            LOG.warn("Exception occurs when deleting directory " + directory, e);
        }
    }

    default long getFileSize(Path path) throws IOException {
        return getFileStatus(path).getLen();
    }

    default boolean isDir(Path path) throws IOException {
        return getFileStatus(path).isDir();
    }

    default void checkOrMkdirs(Path path) throws IOException {
        if (exists(path)) {
            checkArgument(isDir(path), "The path '%s' should be a directory.", path);
        } else {
            mkdirs(path);
        }
    }

    /** Read file to UTF_8 decoding. */
    default String readFileUtf8(Path path) throws IOException {
        try (SeekableInputStream in = newInputStream(path)) {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            return builder.toString();
        }
    }

    /**
     * Write content to one file atomically, initially writes to temp hidden file and only renames
     * to the target file once temp file is closed.
     *
     * @return false if target file exists
     */
    default boolean writeFileUtf8(Path path, String content) throws IOException {
        if (exists(path)) {
            return false;
        }

        Path tmp = path.createTempPath();
        boolean success = false;
        try {
            try (PositionOutputStream out = newOutputStream(tmp, false)) {
                OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
                writer.write(content);
                writer.flush();
            }

            success = rename(tmp, path);
        } finally {
            if (!success) {
                deleteQuietly(tmp);
            }
        }

        return success;
    }

    /**
     * Overwrite file by content atomically, different {@link FileIO}s have different atomic
     * implementations.
     */
    default void overwriteFileUtf8(Path path, String content) throws IOException {
        try (PositionOutputStream out = newOutputStream(path, true)) {
            OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
            writer.write(content);
            writer.flush();
        }
    }

    /**
     * Read file to UTF_8 decoding, then write content to one file atomically, initially writes to
     * temp hidden file and only renames to the target file once temp file is closed.
     *
     * @return false if targetPath file exists
     */
    default boolean copyFileUtf8(Path sourcePath, Path targetPath) throws IOException {
        String content = readFileUtf8(sourcePath);
        return writeFileUtf8(targetPath, content);
    }

    /** Read file from {@link #overwriteFileUtf8} file. */
    default Optional<String> readOverwrittenFileUtf8(Path path) throws IOException {
        int retryNumber = 0;
        IOException exception = null;
        while (retryNumber++ < 5) {
            try {
                if (!exists(path)) {
                    return Optional.empty();
                }

                return Optional.of(readFileUtf8(path));
            } catch (IOException e) {
                if (e.getClass()
                        .getName()
                        .endsWith("org.apache.hadoop.fs.s3a.RemoteFileChangedException")) {
                    // retry for S3 RemoteFileChangedException
                    exception = e;
                } else if (e.getMessage() != null
                        && e.getMessage().contains("Blocklist for")
                        && e.getMessage().contains("has changed")) {
                    // retry for HDFS blocklist has changed exception
                    exception = e;
                } else {
                    throw e;
                }
            }
        }

        throw exception;
    }

    // -------------------------------------------------------------------------
    //                         static creator
    // -------------------------------------------------------------------------

    /**
     * Returns a reference to the {@link FileIO} instance for accessing the file system identified
     * by the given path.
     */
    static FileIO get(Path path, CatalogContext config) throws IOException {
        URI uri = path.toUri();
        if (uri.getScheme() == null) {
            return new LocalFileIO();
        }

        // print a helpful pointer for malformed local URIs (happens a lot to new users)
        if (uri.getScheme().equals("file")
                && uri.getAuthority() != null
                && !uri.getAuthority().isEmpty()) {
            String supposedUri = "file:///" + uri.getAuthority() + uri.getPath();

            throw new IOException(
                    "Found local file path with authority '"
                            + uri.getAuthority()
                            + "' in path '"
                            + uri
                            + "'. Hint: Did you forget a slash? (correct path would be '"
                            + supposedUri
                            + "')");
        }

        FileIOLoader loader = null;
        List<IOException> ioExceptionList = new ArrayList<>();

        // load preferIO
        FileIOLoader preferIOLoader = config.preferIO();
        try {
            loader = checkAccess(preferIOLoader, path, config);
        } catch (IOException ioException) {
            ioExceptionList.add(ioException);
        }

        if (loader == null) {
            Map<String, FileIOLoader> loaders = discoverLoaders();
            loader = loaders.get(uri.getScheme());
        }

        // load fallbackIO
        FileIOLoader fallbackIO = config.fallbackIO();

        if (loader != null) {
            Set<String> options =
                    config.options().keySet().stream()
                            .map(String::toLowerCase)
                            .collect(Collectors.toSet());
            Set<String> missOptions = new HashSet<>();
            for (String[] keys : loader.requiredOptions()) {
                boolean found = false;
                for (String key : keys) {
                    if (options.contains(key.toLowerCase())) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    missOptions.add(keys[0]);
                }
            }
            if (missOptions.size() > 0) {
                IOException exception =
                        new IOException(
                                String.format(
                                        "One or more required options are missing.\n\n"
                                                + "Missing required options are:\n\n"
                                                + "%s",
                                        String.join("\n", missOptions)));
                ioExceptionList.add(exception);
                loader = null;
            }
        }

        if (loader == null) {
            try {
                loader = checkAccess(fallbackIO, path, config);
            } catch (IOException ioException) {
                ioExceptionList.add(ioException);
            }
        }

        // load hadoopIO
        if (loader == null) {
            try {
                loader = checkAccess(new HadoopFileIOLoader(), path, config);
            } catch (IOException ioException) {
                ioExceptionList.add(ioException);
            }
        }

        if (loader == null) {
            String fallbackMsg = "";
            String preferMsg = "";
            if (preferIOLoader != null) {
                preferMsg =
                        " "
                                + preferIOLoader.getClass().getSimpleName()
                                + " also cannot access this path.";
            }
            if (fallbackIO != null) {
                fallbackMsg =
                        " "
                                + fallbackIO.getClass().getSimpleName()
                                + " also cannot access this path.";
            }
            UnsupportedSchemeException ex =
                    new UnsupportedSchemeException(
                            String.format(
                                    "Could not find a file io implementation for scheme '%s' in the classpath."
                                            + "%s %s Hadoop FileSystem also cannot access this path '%s'.",
                                    uri.getScheme(), preferMsg, fallbackMsg, path));
            for (IOException ioException : ioExceptionList) {
                ex.addSuppressed(ioException);
            }

            throw ex;
        }

        FileIO fileIO = loader.load(path);
        fileIO.configure(config);
        return fileIO;
    }

    /** Discovers all {@link FileIOLoader} by service loader. */
    static Map<String, FileIOLoader> discoverLoaders() {
        Map<String, FileIOLoader> results = new HashMap<>();
        Iterator<FileIOLoader> iterator =
                ServiceLoader.load(FileIOLoader.class, FileIOLoader.class.getClassLoader())
                        .iterator();
        iterator.forEachRemaining(
                fileIO -> {
                    FileIOLoader previous = results.put(fileIO.getScheme(), fileIO);
                    if (previous != null) {
                        throw new RuntimeException(
                                String.format(
                                        "Multiple FileIO for scheme '%s' found in the classpath.\n"
                                                + "Ambiguous FileIO classes are:\n"
                                                + "%s\n%s",
                                        fileIO.getScheme(),
                                        previous.getClass().getName(),
                                        fileIO.getClass().getName()));
                    }
                });
        return results;
    }
}
