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
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import java.io.IOException;

import static org.apache.paimon.rest.RESTTokenFileIO.DATA_TOKEN_ENABLED;

/**
 * A {@link org.apache.paimon.fs.FileIO} implementation for testing.
 *
 * <p>It is used to test the RESTFileIO.
 */
public class RESTTestFileIO extends LocalFileIO {

    public static final String TOKEN_UN_EXIST_MSG = "token is null";
    public static final String TOKEN_EXPIRED_MSG = "token is expired";
    public static final String DATA_PATH_CONF_KEY = "rest.test.data-path";
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
        checkDataToken(path);
        return super.newInputStream(path);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        checkDataToken(path);
        return super.newOutputStream(path, overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        checkDataToken(path);
        return super.getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        checkDataToken(path);
        return super.listStatus(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        checkDataToken(path);
        return super.exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        checkDataToken(path);
        return super.delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        checkDataToken(path);
        return super.mkdirs(path);
    }

    @Override
    public boolean renameImpl(Path src, Path dst) throws IOException {
        checkDataToken(src);
        return super.renameImpl(src, dst);
    }

    private void checkDataToken(Path path) throws IOException {
        boolean isDataTokenEnabled = options.getOptional(DATA_TOKEN_ENABLED).orElse(false);
        if (isDataTokenEnabled) {
            RESTToken token = getToken(path);
            if (token == null) {
                throw new IOException(TOKEN_UN_EXIST_MSG);
            } else if (token.expireAtMillis() < System.currentTimeMillis()) {
                throw new IOException(TOKEN_EXPIRED_MSG);
            }
        }
    }

    private RESTToken getToken(Path path) {
        String dataPath = options.get(DATA_PATH_CONF_KEY);
        String basePath = dataPath.replaceAll(RESTFileIOTestLoader.SCHEME + "://", "");
        String filePath = path.toString().split(":")[1].replaceAll(basePath, "");
        String[] paths = filePath.split("/");
        String database = paths[0].replaceAll("\\.db", "");
        String table = paths[1];
        return DataTokenStore.getDataToken(
                options.get(CatalogOptions.WAREHOUSE.key()),
                Identifier.create(database, table).getFullName());
    }
}
