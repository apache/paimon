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

package org.apache.paimon.rest;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;

import java.io.IOException;

/**
 * A {@link org.apache.paimon.fs.FileIO} implementation for testing.
 *
 * <p>It is used to test the RESTFileIO.
 */
public class RESTTestFileIO extends LocalFileIO {
    private Options options;

    @Override
    public boolean isObjectStore() {
        return false;
    }

    @Override
    public void configure(CatalogContext context) {
        options = context.options();
        super.configure(context);
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        getToken(path);
        return super.newInputStream(path);
    }

    private RESTToken getToken(Path path) {
        String basePath = options.get("warehouse").replaceAll("rest-test-file-io://", "");
        String filePath = path.toString().split(":")[1].replaceAll(basePath, "");
        String[] paths = filePath.split("/");
        String database = paths[1].replaceAll("\\.db", "");
        String table = paths[2];
        return DataTokenStore.getDataToken(
                options.get("catalog-server-id"), Identifier.create(database, table).getFullName());
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return super.newOutputStream(path, overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return super.getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return super.listStatus(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return super.exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return super.delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return super.mkdirs(path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return super.rename(src, dst);
    }
}
