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
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.hadoop.HadoopFileIOLoader;
import org.apache.paimon.fs.local.LocalFileIO;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.RESOLVING_FILE_IO_ENABLED;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Provider-neutral file I/O for files and logical directories.
 *
 * <p>Implementations are not required to materialize directory markers for logical directories.
 *
 * @since 0.4.0
 */
@Public
@ThreadSafe
public interface FileIO extends Serializable, Closeable {

    Logger LOG = LoggerFactory.getLogger(FileIO.class);

    boolean isObjectStore();

    /** Configure by {@link CatalogContext}. */
    void configure(CatalogContext context);

    /** Set filesystem options at runtime. Usually used for job-level settings. */
    default void setRuntimeContext(Map<String, String> options) {}

    /**
     * Opens a {@link SeekableInputStream} for a file.
     *
     * <p>The returned stream starts at position zero and supports seeking from zero through the
     * file length, inclusive. Behavior for offsets outside that range is unspecified. If the path
     * is missing or is a directory, this method or the first read from the returned stream throws
     * an {@link IOException}.
     *
     * @param path the file to open
     * @return a seekable stream for the file
     * @throws IOException if the file cannot be read
     */
    SeekableInputStream newInputStream(Path path) throws IOException;

    /**
     * Opens a {@link PositionOutputStream} for a file.
     *
     * <p>When no ancestor is a file, missing logical parents are created. A successful close makes
     * the complete written content visible. If the target exists, {@code overwrite=true} replaces
     * it. With {@code overwrite=false}, the conflict may be reported while opening, writing, or
     * closing the stream, and the existing content remains unchanged.
     *
     * @param path the file to write
     * @param overwrite whether to replace an existing file
     * @return a stream whose position tracks the number of bytes written
     * @throws IOException if the file cannot be written
     */
    PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException;

    /**
     * Opens a {@link TwoPhaseOutputStream} that stages data for later publication.
     *
     * <p>Staged data is not published at the target before commit is invoked. A successful commit
     * publishes the complete data; if commit fails, the target state is unspecified. If the target
     * already exists, whether the request is rejected or replaces the target, when a rejection is
     * reported, and whether replacement is atomic are not specified by this interface. An
     * implementation may document stronger guarantees. The staging layout is also not specified.
     *
     * @param path the file target path
     * @param overwrite requests replacement of an existing file; existing-target behavior is
     *     provider-specific
     * @return a stream that stages data for the target
     * @throws IOException if the stream cannot be created
     * @throws UnsupportedOperationException if the file system does not support staged writes
     */
    default TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        return new RenamingTwoPhaseOutputStream(this, path, overwrite);
    }

    /**
     * Returns a metadata snapshot for a path.
     *
     * @param path the path to inspect
     * @return a snapshot of the path's status
     * @throws FileNotFoundException if the path does not exist
     * @throws IOException if the status cannot be read
     */
    FileStatus getFileStatus(Path path) throws IOException;

    /**
     * Lists the direct children of an existing directory.
     *
     * <p>The result is non-null and unordered. Each status has the child's path and type; file
     * statuses also have the file length. Behavior for a missing path or a file path is
     * unspecified.
     *
     * @param path an existing directory
     * @return the direct child statuses, or an empty array for an empty directory
     */
    FileStatus[] listStatus(Path path) throws IOException;

    /**
     * Lists files under an existing directory.
     *
     * <p>The result is non-null and unordered. It contains the same set of file paths as {@link
     * #listFilesIterative(Path, boolean)} for the same arguments. Behavior for a missing path or a
     * file path is unspecified.
     *
     * @param path an existing directory
     * @param recursive whether to descend into subdirectories
     * @return only file statuses, recursively if requested
     */
    default FileStatus[] listFiles(Path path, boolean recursive) throws IOException {
        List<FileStatus> files = new ArrayList<>();
        RemoteIterator<FileStatus> iter = listFilesIterative(path, recursive);
        while (iter.hasNext()) {
            files.add(iter.next());
        }
        return files.toArray(new FileStatus[0]);
    }

    /**
     * Iterates over files under an existing directory.
     *
     * <p>The iterator is non-null and unordered. It contains the same set of file paths as {@link
     * #listFiles(Path, boolean)} for the same arguments. Behavior for a missing path or a file path
     * is unspecified.
     *
     * @param path an existing directory
     * @param recursive whether to descend into subdirectories
     * @return an iterator containing only file statuses, recursively if requested
     */
    default RemoteIterator<FileStatus> listFilesIterative(Path path, boolean recursive)
            throws IOException {
        Queue<FileStatus> files = new LinkedList<>();
        Queue<Path> directories = new LinkedList<>(Collections.singletonList(path));
        return new RemoteIterator<FileStatus>() {

            @Override
            public boolean hasNext() throws IOException {
                maybeUnpackDirectory();
                return !files.isEmpty();
            }

            @Override
            public FileStatus next() throws IOException {
                maybeUnpackDirectory();
                return files.remove();
            }

            private void maybeUnpackDirectory() throws IOException {
                while (files.isEmpty() && !directories.isEmpty()) {
                    FileStatus[] statuses = listStatus(directories.remove());
                    for (FileStatus f : statuses) {
                        if (!f.isDir()) {
                            files.add(f);
                            continue;
                        }
                        if (!recursive) {
                            continue;
                        }
                        directories.add(f.getPath());
                    }
                }
            }
        };
    }

    /**
     * Lists the direct directories under an existing directory.
     *
     * <p>The result is non-null and unordered. Behavior for a missing path or a file path is
     * unspecified.
     *
     * @param path an existing directory
     * @return only direct child directory statuses, or an empty array if there are none
     */
    default FileStatus[] listDirectories(Path path) throws IOException {
        FileStatus[] statuses = listStatus(path);
        if (statuses != null) {
            statuses = Arrays.stream(statuses).filter(FileStatus::isDir).toArray(FileStatus[]::new);
        }
        return statuses;
    }

    /**
     * Checks whether a file or logical directory exists.
     *
     * @param path the path to check
     * @return whether the path exists
     */
    boolean exists(Path path) throws IOException;

    /**
     * Deletes a file or directory.
     *
     * <p>An existing file is deleted for either value of {@code recursive}. An empty directory is
     * deleted, and a non-empty directory is deleted with its subtree when {@code recursive} is
     * true. These successful deletions return true. Deleting a non-empty directory with {@code
     * recursive=false} throws an {@link IOException} and preserves the complete tree. A missing
     * path does not throw solely because it is absent; its return value is unspecified.
     *
     * @param path the path to delete
     * @param recursive whether to delete a directory subtree
     * @return true when an existing target is successfully deleted; unspecified for a missing path
     */
    boolean delete(Path path, boolean recursive) throws IOException;

    /**
     * Makes a logical directory and any missing logical parents.
     *
     * <p>When the target and its ancestors are not files, this method returns true and leaves them
     * as directories. Repeating the call also returns true. Implementations need not materialize
     * directory markers.
     *
     * @param path the directory to create
     * @return true on successful creation or when the directory already exists
     * @throws IOException if the directory cannot be created
     */
    boolean mkdirs(Path path) throws IOException;

    /**
     * Renames a file or directory in the guaranteed non-conflicting case.
     *
     * <p>When the source exists, the exact destination is missing, and the destination parent is
     * valid, this method returns true, removes the source, and preserves the file bytes or
     * directory tree at the exact destination. Behavior in all other cases, including destination
     * conflicts, is unspecified. Atomicity is not guaranteed.
     *
     * @param src the source file or directory
     * @param dst the exact destination path
     * @return true for the guaranteed case; otherwise unspecified
     */
    boolean rename(Path src, Path dst) throws IOException;

    default Optional<Path> archive(Path path, StorageType type) throws IOException {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support archive.");
    }

    default void restoreArchive(Path path, Duration duration) throws IOException {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support restore archive.");
    }

    default Optional<Path> unarchive(Path path, StorageType type) throws IOException {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support unarchive.");
    }

    default String createBlobPresignedUrl(
            Path tableRoot, BlobDescriptor descriptor, Duration validity) throws IOException {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support creating blob presigned URLs.");
    }

    /**
     * Override this method to empty, many FileIO implementation classes rely on static variables
     * and do not have the ability to close them.
     */
    @Override
    default void close() throws IOException {}

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

    default void deleteFilesQuietly(List<Path> files) {
        for (Path file : files) {
            deleteQuietly(file);
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
     * Writes content through a temporary file and then renames it to the target.
     *
     * <p>If the target is missing and its parent is valid, a true result guarantees the target has
     * the requested content. This method does not add an atomicity or conflict guarantee beyond
     * {@link #rename(Path, Path)}.
     *
     * @return whether the final rename reported success
     */
    default boolean tryToWriteAtomic(Path path, String content) throws IOException {
        Path tmp = path.createTempPath();
        boolean success = false;
        try {
            writeFile(tmp, content, false);
            success = rename(tmp, path);
        } finally {
            if (!success) {
                deleteQuietly(tmp);
            }
        }

        return success;
    }

    default void writeFile(Path path, String content, boolean overwrite) throws IOException {
        try (PositionOutputStream out = newOutputStream(path, overwrite)) {
            OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
            writer.write(content);
            writer.flush();
        }
    }

    /** Overwrites a file with UTF-8 content without guaranteeing atomic replacement. */
    default void overwriteFileUtf8(Path path, String content) throws IOException {
        try (PositionOutputStream out = newOutputStream(path, true)) {
            OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
            writer.write(content);
            writer.flush();
        }
    }

    /** Overwrites a hint file with UTF-8 content without guaranteeing atomic replacement. */
    default void overwriteHintFile(Path path, String content) throws IOException {
        overwriteFileUtf8(path, content);
    }

    /**
     * Copies the bytes of a source file to a target file.
     *
     * <p>If the target exists, {@code overwrite=true} replaces it. With {@code overwrite=false},
     * the copy fails and preserves the existing target content.
     *
     * @param sourcePath the source file
     * @param targetPath the target file
     * @param overwrite whether to replace an existing target
     * @throws IOException if the file cannot be copied or overwrite is disabled for an existing
     *     target
     */
    default void copyFile(Path sourcePath, Path targetPath, boolean overwrite) throws IOException {
        try (SeekableInputStream is = newInputStream(sourcePath);
                PositionOutputStream os = newOutputStream(targetPath, overwrite)) {
            IOUtils.copy(is, os);
        }
    }

    /**
     * Copies every direct file from a source directory to a target directory according to {@link
     * #copyFile(Path, Path, boolean)}.
     *
     * <p>{@code sourceDirectory} and {@code targetDirectory} must be directories, and each direct
     * child of {@code sourceDirectory} must be a file. The method is not recursive and does not
     * roll back files copied before a later failure.
     */
    default void copyFiles(Path sourceDirectory, Path targetDirectory, boolean overwrite)
            throws IOException {
        FileStatus[] fileStatuses = listStatus(sourceDirectory);
        List<Path> copyFiles =
                Arrays.stream(fileStatuses).map(FileStatus::getPath).collect(Collectors.toList());
        for (Path file : copyFiles) {
            String fileName = file.getName();
            Path targetPath = new Path(targetDirectory.toString() + "/" + fileName);
            copyFile(file, targetPath, overwrite);
        }
    }

    /** Read file from {@link #overwriteFileUtf8} file. */
    default Optional<String> readOverwrittenFileUtf8(Path path) throws IOException {
        int retryNumber = 0;
        Exception exception = null;
        while (retryNumber++ < 5) {
            try {
                return Optional.of(readFileUtf8(path));
            } catch (FileNotFoundException e) {
                return Optional.empty();
            } catch (Exception e) {
                if (!exists(path)) {
                    return Optional.empty();
                }

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

        if (exception instanceof IOException) {
            throw (IOException) exception;
        }
        throw new RuntimeException(exception);
    }

    // -------------------------------------------------------------------------
    //                         static creator
    // -------------------------------------------------------------------------

    /**
     * Returns a reference to the {@link FileIO} instance for accessing the file system identified
     * by the given path.
     */
    static FileIO get(Path path, CatalogContext config) throws IOException {
        if (config.options().get(RESOLVING_FILE_IO_ENABLED)) {
            FileIO fileIO = new ResolvingFileIO();
            fileIO.configure(config);
            return fileIO;
        }

        URI uri = path.toUri();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting FileIO by scheme {}.", uri.getScheme());
        }

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
            if (loader != null && LOG.isDebugEnabled()) {
                LOG.debug(
                        "Found preferIOLoader {} with scheme {}.",
                        loader.getClass().getName(),
                        loader.getScheme());
            }
        } catch (IOException ioException) {
            ioExceptionList.add(ioException);
        }

        if (loader == null) {
            Map<String, FileIOLoader> loaders = discoverLoaders();
            loader = loaders.get(uri.getScheme());
            if (!loaders.isEmpty() && LOG.isDebugEnabled()) {
                LOG.debug(
                        "Discovered FileIOLoaders: {}.",
                        loaders.entrySet().stream()
                                .map(
                                        e ->
                                                String.format(
                                                        "{%s,%s}",
                                                        e.getKey(),
                                                        e.getValue().getClass().getName()))
                                .collect(Collectors.joining(",")));
            }
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
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Got {} but miss options. Will try to get fallback IO and Hadoop IO respectively.",
                            loader.getClass().getName());
                }
                loader = null;
            }
        }

        if (loader == null) {
            try {
                loader = checkAccess(fallbackIO, path, config);
                if (loader != null && LOG.isDebugEnabled()) {
                    LOG.debug("Got fallback FileIOLoader: {}.", loader.getClass().getName());
                }
            } catch (IOException ioException) {
                ioExceptionList.add(ioException);
            }
        }

        // load hadoopIO
        if (loader == null) {
            try {
                loader = checkAccess(new HadoopFileIOLoader(), path, config);
                if (loader != null && LOG.isDebugEnabled()) {
                    LOG.debug("Got hadoop FileIOLoader: {}.", loader.getClass().getName());
                }
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

    static FileIOLoader checkAccess(FileIOLoader fileIO, Path path, CatalogContext config)
            throws IOException {
        if (fileIO == null) {
            return null;
        }

        // check access
        FileIO io = fileIO.load(path);
        io.configure(config);
        io.exists(path);
        return fileIO;
    }
}
