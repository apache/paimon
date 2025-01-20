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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.auth.AuthSession;
import org.apache.paimon.rest.responses.GetTableCredentialsResponse;

import java.io.IOException;
import java.util.Map;

/** A {@link FileIO} to support refresh credential. */
public class RefreshCredentialFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    private final ResourcePaths resourcePaths;
    private final AuthSession catalogAuth;
    protected Options options;
    private final Identifier identifier;
    private Long expireAtMillis;
    private Map<String, String> credential;
    private final transient RESTClient client;
    private transient volatile FileIO lazyFileIO;

    public RefreshCredentialFileIO(
            ResourcePaths resourcePaths,
            AuthSession catalogAuth,
            Options options,
            RESTClient client,
            Identifier identifier) {
        this.resourcePaths = resourcePaths;
        this.catalogAuth = catalogAuth;
        this.options = options;
        this.identifier = identifier;
        this.client = client;
    }

    @Override
    public void configure(CatalogContext context) {
        this.options = context.options();
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return fileIO().newInputStream(path);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return fileIO().newOutputStream(path, overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return fileIO().getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return fileIO().listStatus(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return fileIO().exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return fileIO().delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return fileIO().mkdirs(path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return fileIO().rename(src, dst);
    }

    @Override
    public boolean isObjectStore() {
        try {
            return fileIO().isObjectStore();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private FileIO fileIO() throws IOException {
        if (lazyFileIO == null || shouldRefresh()) {
            synchronized (this) {
                if (lazyFileIO == null || shouldRefresh()) {
                    GetTableCredentialsResponse response = getCredential();
                    expireAtMillis = response.getExpiresAtMillis();
                    credential = response.getCredential();
                    Map<String, String> conf = RESTUtil.merge(options.toMap(), credential);
                    Options updateCredentialOption = new Options(conf);
                    lazyFileIO =
                            FileIO.get(
                                    new Path(updateCredentialOption.get(CatalogOptions.WAREHOUSE)),
                                    CatalogContext.create(updateCredentialOption));
                }
            }
        }
        return lazyFileIO;
    }

    // todo: handle exception
    private GetTableCredentialsResponse getCredential() {
        return client.get(
                resourcePaths.tableCredentials(
                        identifier.getDatabaseName(), identifier.getObjectName()),
                GetTableCredentialsResponse.class,
                catalogAuth.getHeaders());
    }

    private boolean shouldRefresh() {
        return expireAtMillis != null && expireAtMillis > System.currentTimeMillis();
    }
}
