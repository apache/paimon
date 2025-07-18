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

package org.apache.paimon.vfs;

import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.paimon.CoreOptions.PATH;

/** Identifier for table. */
public abstract class VFSTableIdentifier extends VFSIdentifier {
    protected Path realPath;
    protected String scheme;
    protected URI realUri;
    protected String tableName;
    protected TableMetadata table;
    protected FileIO fileIO;
    protected String tableLocation;

    // Constructor for non-exist table
    public VFSTableIdentifier(VFSFileType vfsFileType, String databaseName, String tableName) {
        super(vfsFileType, databaseName);
        this.tableName = tableName;
    }

    // Constructor for existing table
    public VFSTableIdentifier(
            VFSFileType vfsFileType,
            TableMetadata table,
            String realPath,
            FileIO fileIO,
            String databaseName,
            String tableName) {
        super(vfsFileType, databaseName);
        this.realPath = new Path(realPath);
        try {
            this.realUri = new URI(realPath);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        this.scheme = realUri.getScheme();
        this.tableName = tableName;
        this.table = table;
        this.fileIO = fileIO;
        if (table != null) {
            Options options = new Options(table.schema().options());
            this.tableLocation = options.get(PATH);
        }
    }

    public Path getRealPath() {
        return realPath;
    }

    public URI getRealUri() {
        return realUri;
    }

    public String getScheme() {
        return scheme;
    }

    public String getTableName() {
        return tableName;
    }

    public TableMetadata getTable() {
        return table;
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public boolean isTableExist() {
        return table != null;
    }
}
