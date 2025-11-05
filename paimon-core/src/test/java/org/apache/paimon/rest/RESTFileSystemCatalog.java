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
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;

import static org.apache.paimon.CoreOptions.PATH;

/**
 * A FileSystemCatalog that supports custom table paths for REST catalog server. This allows REST
 * catalog to create external tables with specified paths.
 */
public class RESTFileSystemCatalog extends FileSystemCatalog {

    public RESTFileSystemCatalog(FileIO fileIO, Path warehouse, CatalogContext context) {
        super(fileIO, warehouse, context);
    }

    @Override
    protected boolean allowCustomTablePath() {
        return true;
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        boolean isExternal = schema.options() != null && schema.options().containsKey(PATH.key());
        if (!isExternal) {
            super.createTable(identifier, schema, ignoreIfExists);
        }
    }
}
