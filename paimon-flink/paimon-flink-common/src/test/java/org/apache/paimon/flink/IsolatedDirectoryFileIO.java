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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;

import java.io.IOException;

/** An isolated LocalFileIO, only permitting accessing files under the root directory. */
public class IsolatedDirectoryFileIO extends LocalFileIO {

    public static final String ROOT_DIR = "root-dir";

    private Path root;

    public IsolatedDirectoryFileIO() {}

    @Override
    public void configure(CatalogContext context) {
        root = new Path(context.options().get(ROOT_DIR));
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        checkPath(path);
        return super.newInputStream(path);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        checkPath(path);
        return super.newOutputStream(path, overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        checkPath(path);
        return super.getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        checkPath(path);
        return super.listStatus(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        checkPath(path);
        return super.exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        checkPath(path);
        return super.delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        checkPath(path);
        return super.mkdirs(path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        checkPath(src);
        checkPath(dst);
        return super.rename(src, dst);
    }

    @Override
    public void copyFile(Path sourcePath, Path targetPath, boolean overwrite) throws IOException {
        checkPath(sourcePath);
        checkPath(targetPath);
        super.copyFile(sourcePath, targetPath, overwrite);
    }

    private void checkPath(Path path) {
        if (path == null) {
            throw new NullPointerException("path is null");
        }
        if (!path.toString().startsWith(root.toString())) {
            throw new UnsupportedOperationException(
                    "Isolated file io only supports reading child of root directory "
                            + root
                            + ", but current accessing path is "
                            + path);
        }
    }

    /** FileIOLoader of {@link IsolatedDirectoryFileIO}. */
    public static final class Loader implements FileIOLoader {

        private static final long serialVersionUID = 1L;

        @Override
        public String getScheme() {
            return "isolated";
        }

        @Override
        public FileIO load(Path path) {
            return new IsolatedDirectoryFileIO();
        }
    }
}
