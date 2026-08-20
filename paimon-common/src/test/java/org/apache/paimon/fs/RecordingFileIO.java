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

import org.apache.paimon.catalog.CatalogContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Deterministic in-memory implementation of the primitive {@link FileIO} operations. */
final class RecordingFileIO implements FileIO {

    enum Method {
        GET_FILE_STATUS,
        LIST_STATUS,
        EXISTS,
        DELETE,
        MKDIRS,
        RENAME,
        NEW_INPUT_STREAM,
        NEW_OUTPUT_STREAM,
        INPUT_READ,
        OUTPUT_WRITE
    }

    static final class Call {
        private final Method method;
        private final List<Object> arguments;

        private Call(Method method, Object... arguments) {
            this.method = method;
            this.arguments = Arrays.asList(arguments);
        }

        Method method() {
            return method;
        }

        <T> T argument(int index, Class<T> type) {
            return type.cast(arguments.get(index));
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Call)) {
                return false;
            }
            Call that = (Call) other;
            return method == that.method && arguments.equals(that.arguments);
        }

        @Override
        public int hashCode() {
            return Objects.hash(method, arguments);
        }

        @Override
        public String toString() {
            return method + arguments.toString();
        }
    }

    private final Map<Path, byte[]> files = new LinkedHashMap<>();
    private final Set<Path> directories = new LinkedHashSet<>();
    private final List<Call> calls = new ArrayList<>();
    private final Map<Method, Deque<IOException>> failures = new EnumMap<>(Method.class);
    private int openInputStreams;
    private int openOutputStreams;

    static Call call(Method method, Object... arguments) {
        return new Call(method, arguments);
    }

    void putFile(Path path, String content) {
        addParentDirectories(path);
        files.put(path, content.getBytes(StandardCharsets.UTF_8));
    }

    void putDirectory(Path path) {
        addParentDirectories(path);
        directories.add(path);
    }

    String fileContent(Path path) {
        return new String(files.get(path), StandardCharsets.UTF_8);
    }

    List<Call> calls() {
        return new ArrayList<>(calls);
    }

    List<Call> calls(Method method) {
        return calls.stream().filter(call -> call.method == method).collect(Collectors.toList());
    }

    long callCount(Method method) {
        return calls.stream().filter(call -> call.method == method).count();
    }

    boolean existsInMemory(Path path) {
        return files.containsKey(path) || directories.contains(path);
    }

    boolean isDirectoryInMemory(Path path) {
        return directories.contains(path);
    }

    int openInputStreams() {
        return openInputStreams;
    }

    int openOutputStreams() {
        return openOutputStreams;
    }

    void failNext(Method method, IOException failure) {
        failures.computeIfAbsent(method, ignored -> new ArrayDeque<>()).add(failure);
    }

    void reset() {
        calls.clear();
        failures.clear();
    }

    @Override
    public boolean isObjectStore() {
        return false;
    }

    @Override
    public void configure(CatalogContext context) {}

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        calls.add(call(Method.NEW_INPUT_STREAM, path));
        maybeFail(Method.NEW_INPUT_STREAM);
        byte[] content = files.get(path);
        if (content == null) {
            throw new FileNotFoundException(path.toString());
        }
        openInputStreams++;
        return new SeekableInputStream() {
            private final ByteArrayInputStream input = new ByteArrayInputStream(content);
            private long position;
            private boolean closed;

            @Override
            public void seek(long desired) throws IOException {
                if (desired < 0 || desired > content.length) {
                    throw new IOException("Invalid seek position " + desired);
                }
                input.reset();
                long skipped = input.skip(desired);
                if (skipped != desired) {
                    throw new IOException("Could not seek to " + desired);
                }
                position = desired;
            }

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public int read() throws IOException {
                maybeFail(Method.INPUT_READ);
                int value = input.read();
                if (value >= 0) {
                    position++;
                }
                return value;
            }

            @Override
            public int read(byte[] bytes, int offset, int length) throws IOException {
                maybeFail(Method.INPUT_READ);
                int read = input.read(bytes, offset, length);
                if (read > 0) {
                    position += read;
                }
                return read;
            }

            @Override
            public void close() {
                if (!closed) {
                    closed = true;
                    openInputStreams--;
                }
            }
        };
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        calls.add(call(Method.NEW_OUTPUT_STREAM, path, overwrite));
        maybeFail(Method.NEW_OUTPUT_STREAM);
        if (!overwrite && existsInMemory(path)) {
            throw new FileAlreadyExistsException(path.toString());
        }
        openOutputStreams++;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        return new PositionOutputStream() {
            private boolean closed;

            @Override
            public long getPos() {
                return output.size();
            }

            @Override
            public void write(int value) throws IOException {
                maybeFail(Method.OUTPUT_WRITE);
                output.write(value);
            }

            @Override
            public void write(byte[] bytes) throws IOException {
                maybeFail(Method.OUTPUT_WRITE);
                output.write(bytes);
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
                maybeFail(Method.OUTPUT_WRITE);
                output.write(bytes, offset, length);
            }

            @Override
            public void flush() throws IOException {
                output.flush();
            }

            @Override
            public void close() {
                if (!closed) {
                    closed = true;
                    addParentDirectories(path);
                    files.put(path, output.toByteArray());
                    openOutputStreams--;
                }
            }
        };
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        calls.add(call(Method.GET_FILE_STATUS, path));
        maybeFail(Method.GET_FILE_STATUS);
        if (files.containsKey(path)) {
            return new MemoryFileStatus(path, false, files.get(path).length);
        }
        if (directories.contains(path)) {
            return new MemoryFileStatus(path, true, 0);
        }
        throw new FileNotFoundException(path.toString());
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        calls.add(call(Method.LIST_STATUS, path));
        maybeFail(Method.LIST_STATUS);
        if (!directories.contains(path)) {
            throw new FileNotFoundException(path.toString());
        }
        List<FileStatus> statuses = new ArrayList<>();
        for (Path directory : directories) {
            if (!directory.equals(path) && path.equals(directory.getParent())) {
                statuses.add(new MemoryFileStatus(directory, true, 0));
            }
        }
        for (Map.Entry<Path, byte[]> file : files.entrySet()) {
            if (path.equals(file.getKey().getParent())) {
                statuses.add(new MemoryFileStatus(file.getKey(), false, file.getValue().length));
            }
        }
        statuses.sort(Comparator.comparing(FileStatus::getPath));
        return statuses.toArray(new FileStatus[0]);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        calls.add(call(Method.EXISTS, path));
        maybeFail(Method.EXISTS);
        return existsInMemory(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        calls.add(call(Method.DELETE, path, recursive));
        maybeFail(Method.DELETE);
        if (files.remove(path) != null) {
            return true;
        }
        if (!directories.contains(path)) {
            return false;
        }
        boolean hasChildren =
                files.keySet().stream().anyMatch(child -> isDescendant(child, path))
                        || directories.stream()
                                .anyMatch(
                                        child -> !child.equals(path) && isDescendant(child, path));
        if (hasChildren && !recursive) {
            return false;
        }
        files.keySet().removeIf(child -> isDescendant(child, path));
        directories.removeIf(child -> child.equals(path) || isDescendant(child, path));
        return true;
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        calls.add(call(Method.MKDIRS, path));
        maybeFail(Method.MKDIRS);
        boolean missing = !directories.contains(path);
        putDirectory(path);
        return missing;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        calls.add(call(Method.RENAME, src, dst));
        maybeFail(Method.RENAME);
        if (existsInMemory(dst)) {
            return false;
        }
        byte[] content = files.remove(src);
        if (content == null) {
            return false;
        }
        addParentDirectories(dst);
        files.put(dst, content);
        return true;
    }

    private void maybeFail(Method method) throws IOException {
        Deque<IOException> scripted = failures.get(method);
        if (scripted != null && !scripted.isEmpty()) {
            throw scripted.remove();
        }
    }

    private void addParentDirectories(Path path) {
        Path parent = path.getParent();
        while (parent != null) {
            directories.add(parent);
            parent = parent.getParent();
        }
    }

    private static boolean isDescendant(Path candidate, Path parent) {
        Path current = candidate.getParent();
        while (current != null) {
            if (current.equals(parent)) {
                return true;
            }
            current = current.getParent();
        }
        return false;
    }

    private static final class MemoryFileStatus implements FileStatus {
        private final Path path;
        private final boolean directory;
        private final long length;

        private MemoryFileStatus(Path path, boolean directory, long length) {
            this.path = path;
            this.directory = directory;
            this.length = length;
        }

        @Override
        public long getLen() {
            return length;
        }

        @Override
        public boolean isDir() {
            return directory;
        }

        @Override
        public Path getPath() {
            return path;
        }

        @Override
        public long getModificationTime() {
            return 0;
        }
    }
}
