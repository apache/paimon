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

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * This is a utility class to deal files and directories. Contains utilities for recursive deletion
 * and creation of temporary files.
 */
public class FileIOUtils {

    /** Global lock to prevent concurrent directory deletes under Windows and MacOS. */
    private static final Object DELETE_LOCK = new Object();

    /**
     * The maximum size of array to allocate for reading. See {@code MAX_BUFFER_SIZE} in {@link
     * java.nio.file.Files} for more.
     */
    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    /** The size of the buffer used for reading. */
    private static final int BUFFER_SIZE = 4096;

    // ------------------------------------------------------------------------

    public static void writeCompletely(WritableByteChannel channel, ByteBuffer src)
            throws IOException {
        while (src.hasRemaining()) {
            channel.write(src);
        }
    }

    // ------------------------------------------------------------------------
    //  Simple reading and writing of files
    // ------------------------------------------------------------------------

    public static String readFile(File file, String charsetName) throws IOException {
        byte[] bytes = readAllBytes(file.toPath());
        return new String(bytes, charsetName);
    }

    public static String readFileUtf8(File file) throws IOException {
        return readFile(file, "UTF-8");
    }

    public static void writeFile(File file, String contents, String encoding) throws IOException {
        byte[] bytes = contents.getBytes(encoding);
        Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);
    }

    public static void writeFileUtf8(File file, String contents) throws IOException {
        writeFile(file, contents, "UTF-8");
    }

    public static void writeByteArrayToFile(File file, byte[] data) throws IOException {
        writeByteArrayToFile(file, data, false);
    }

    public static void writeByteArrayToFile(File file, byte[] data, boolean append)
            throws IOException {
        OutputStream out = null;

        try {
            out = openOutputStream(file, append);
            out.write(data);
            out.close();
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    public static FileOutputStream openOutputStream(File file, boolean append) throws IOException {
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new IOException("File '" + file + "' exists but is a directory");
            }

            if (!file.canWrite()) {
                throw new IOException("File '" + file + "' cannot be written to");
            }
        } else {
            File parent = file.getParentFile();
            if (parent != null && !parent.mkdirs() && !parent.isDirectory()) {
                throw new IOException("Directory '" + parent + "' could not be created");
            }
        }

        return new FileOutputStream(file, append);
    }

    /**
     * Reads all the bytes from a file. The method ensures that the file is closed when all bytes
     * have been read or an I/O error, or other runtime exception, is thrown.
     *
     * <p>This is an implementation that follow {@link
     * java.nio.file.Files#readAllBytes(java.nio.file.Path)}, and the difference is that it limits
     * the size of the direct buffer to avoid direct-buffer OutOfMemoryError. When {@link
     * java.nio.file.Files#readAllBytes(java.nio.file.Path)} or other interfaces in java API can do
     * this in the future, we should remove it.
     *
     * @param path the path to the file
     * @return a byte array containing the bytes read from the file
     * @throws IOException if an I/O error occurs reading from the stream
     * @throws OutOfMemoryError if an array of the required size cannot be allocated, for example
     *     the file is larger that {@code 2GB}
     */
    public static byte[] readAllBytes(java.nio.file.Path path) throws IOException {
        try (SeekableByteChannel channel = Files.newByteChannel(path);
                InputStream in = Channels.newInputStream(channel)) {

            long size = channel.size();
            if (size > (long) MAX_BUFFER_SIZE) {
                throw new OutOfMemoryError("Required array size too large");
            }

            return read(in, (int) size);
        }
    }

    /**
     * Reads all the bytes from an input stream. Uses {@code initialSize} as a hint about how many
     * bytes the stream will have and uses {@code directBufferSize} to limit the size of the direct
     * buffer used to read.
     *
     * @param source the input stream to read from
     * @param initialSize the initial size of the byte array to allocate
     * @return a byte array containing the bytes read from the file
     * @throws IOException if an I/O error occurs reading from the stream
     * @throws OutOfMemoryError if an array of the required size cannot be allocated
     */
    private static byte[] read(InputStream source, int initialSize) throws IOException {
        int capacity = initialSize;
        byte[] buf = new byte[capacity];
        int nread = 0;
        int n;

        for (; ; ) {
            // read to EOF which may read more or less than initialSize (eg: file
            // is truncated while we are reading)
            while ((n = source.read(buf, nread, Math.min(capacity - nread, BUFFER_SIZE))) > 0) {
                nread += n;
            }

            // if last call to source.read() returned -1, we are done
            // otherwise, try to read one more byte; if that failed we're done too
            if (n < 0 || (n = source.read()) < 0) {
                break;
            }

            // one more byte was read; need to allocate a larger buffer
            if (capacity <= MAX_BUFFER_SIZE - capacity) {
                capacity = Math.max(capacity << 1, BUFFER_SIZE);
            } else {
                if (capacity == MAX_BUFFER_SIZE) {
                    throw new OutOfMemoryError("Required array size too large");
                }
                capacity = MAX_BUFFER_SIZE;
            }
            buf = Arrays.copyOf(buf, capacity);
            buf[nread++] = (byte) n;
        }
        return (capacity == nread) ? buf : Arrays.copyOf(buf, nread);
    }

    // ------------------------------------------------------------------------
    //  Deleting directories on standard File Systems
    // ------------------------------------------------------------------------

    /**
     * Removes the given file or directory recursively.
     *
     * <p>If the file or directory does not exist, this does not throw an exception, but simply does
     * nothing. It considers the fact that a file-to-be-deleted is not present a success.
     *
     * <p>This method is safe against other concurrent deletion attempts.
     *
     * @param file The file or directory to delete.
     * @throws IOException Thrown if the directory could not be cleaned for some reason, for example
     *     due to missing access/write permissions.
     */
    public static void deleteFileOrDirectory(File file) throws IOException {
        checkNotNull(file, "file");

        guardIfNotThreadSafe(FileIOUtils::deleteFileOrDirectoryInternal, file);
    }

    /**
     * Deletes the given directory recursively.
     *
     * <p>If the directory does not exist, this does not throw an exception, but simply does
     * nothing. It considers the fact that a directory-to-be-deleted is not present a success.
     *
     * <p>This method is safe against other concurrent deletion attempts.
     *
     * @param directory The directory to be deleted.
     * @throws IOException Thrown if the given file is not a directory, or if the directory could
     *     not be deleted for some reason, for example due to missing access/write permissions.
     */
    public static void deleteDirectory(File directory) throws IOException {
        checkNotNull(directory, "directory");

        guardIfNotThreadSafe(FileIOUtils::deleteDirectoryInternal, directory);
    }

    private static void deleteDirectoryInternal(File directory) throws IOException {
        if (directory.isDirectory()) {
            // directory exists and is a directory

            // empty the directory first
            try {
                cleanDirectoryInternal(directory);
            } catch (FileNotFoundException ignored) {
                // someone concurrently deleted the directory, nothing to do for us
                return;
            }

            // delete the directory. this fails if the directory is not empty, meaning
            // if new files got concurrently created. we want to fail then.
            // if someone else deleted the empty directory concurrently, we don't mind
            // the result is the same for us, after all
            Files.deleteIfExists(directory.toPath());
        } else if (directory.exists()) {
            // exists but is file, not directory
            // either an error from the caller, or concurrently a file got created
            throw new IOException(directory + " is not a directory");
        }
        // else: does not exist, which is okay (as if deleted)
    }

    private static void cleanDirectoryInternal(File directory) throws IOException {
        if (Files.isSymbolicLink(directory.toPath())) {
            // the user directories which symbolic links point to should not be cleaned.
            return;
        }
        if (directory.isDirectory()) {
            final File[] files = directory.listFiles();

            if (files == null) {
                // directory does not exist any more or no permissions
                if (directory.exists()) {
                    throw new IOException("Failed to list contents of " + directory);
                } else {
                    throw new FileNotFoundException(directory.toString());
                }
            }

            // remove all files in the directory
            for (File file : files) {
                if (file != null) {
                    deleteFileOrDirectory(file);
                }
            }
        } else if (directory.exists()) {
            throw new IOException(directory + " is not a directory but a regular file");
        } else {
            // else does not exist at all
            throw new FileNotFoundException(directory.toString());
        }
    }

    private static void deleteFileOrDirectoryInternal(File file) throws IOException {
        if (file.isDirectory()) {
            // file exists and is directory
            deleteDirectoryInternal(file);
        } else {
            // if the file is already gone (concurrently), we don't mind
            Files.deleteIfExists(file.toPath());
        }
        // else: already deleted
    }

    private static void guardIfNotThreadSafe(ThrowingConsumer<File, IOException> toRun, File file)
            throws IOException {
        if (OperatingSystem.isWindows()) {
            guardIfWindows(toRun, file);
            return;
        }
        if (OperatingSystem.isMac()) {
            guardIfMac(toRun, file);
            return;
        }

        toRun.accept(file);
    }

    // for Windows, we synchronize on a global lock, to prevent concurrent delete issues
    // >
    // in the future, we may want to find either a good way of working around file visibility
    // under concurrent operations (the behavior seems completely unpredictable)
    // or  make this locking more fine grained, for example  on directory path prefixes
    private static void guardIfWindows(ThrowingConsumer<File, IOException> toRun, File file)
            throws IOException {
        synchronized (DELETE_LOCK) {
            for (int attempt = 1; attempt <= 10; attempt++) {
                try {
                    toRun.accept(file);
                    break;
                } catch (AccessDeniedException e) {
                    // ah, windows...
                }

                // briefly wait and fall through the loop
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    // restore the interruption flag and error out of the method
                    Thread.currentThread().interrupt();
                    throw new IOException("operation interrupted");
                }
            }
        }
    }

    // Guard Mac for the same reason we guard windows. Refer to guardIfWindows for details.
    // The difference to guardIfWindows is that we don't swallow the AccessDeniedException because
    // doing that would lead to wrong behaviour.
    private static void guardIfMac(ThrowingConsumer<File, IOException> toRun, File file)
            throws IOException {
        synchronized (DELETE_LOCK) {
            toRun.accept(file);
            // briefly wait and fall through the loop
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // restore the interruption flag and error out of the method
                Thread.currentThread().interrupt();
                throw new IOException("operation interrupted");
            }
        }
    }

    /**
     * Deletes the given directory recursively, not reporting any I/O exceptions that occur.
     *
     * <p>This method is identical to {@link FileIOUtils#deleteDirectory(File)}, except that it
     * swallows all exceptions and may leave the job quietly incomplete.
     *
     * @param directory The directory to delete.
     */
    public static void deleteDirectoryQuietly(File directory) {
        if (directory == null) {
            return;
        }

        // delete and do not report if it fails
        try {
            deleteDirectory(directory);
        } catch (Exception ignored) {
        }
    }
}
